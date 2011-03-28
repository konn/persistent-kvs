{-# LANGUAGE TypeSynonymInstances, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Database.Persist.Memcached (Memcache, runMemcache) where
import Database.Persist.GenericKVS
import Network (connectTo, HostName, PortNumber, PortID(..))
import Control.Monad.Reader
import System.IO ( Handle(..), hSetNewlineMode, hFlush
                 , hClose, Newline(..), NewlineMode(..))
import Data.ByteString.Char8 hiding (head, map, snoc)
import Data.ByteString (snoc)
import Control.Monad.IO.Class
import Control.Monad.IO.Control
import Control.Exception.Control
import Data.Int
import Data.Maybe
import qualified Text.Show.ByteString as Show
import Control.Applicative
import Prelude hiding ( length, putStrLn
                      , unwords, show, words
                      , concat, getLine, null
                      )
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Data
import qualified Data.Enumerator as E
import Data.Enumerator hiding (map, length)
import qualified Data.Enumerator.Binary as BE
import qualified Data.Enumerator.List as LE
import Control.Concurrent hiding (yield)

toStrict :: LBS.ByteString -> ByteString
toStrict = concat . LBS.toChunks

show :: Show.Show a => a -> ByteString
show = toStrict . Show.show

type Memcache m = ReaderT (MVar Handle) m

data MemcacheError = UnknownCommand
                   | ClientError ByteString
                   | ServerError ByteString
                   | ProtocolError String
                     deriving (Data, Typeable, Show, Eq)

instance Exception MemcacheError

enumLine :: Monad m => Enumeratee ByteString ByteString m b
enumLine = loop
  where
    loop = checkDone (step "")
    step rem k = do
      eof <- E.isEOF
      if eof
        then yield (Continue k) EOF
        else do
          bytes <- concat . LBS.toChunks <$> BE.takeWhile (/= 13)
          BE.drop 1
          h <- BE.head
          case h of
            Just 10 -> k (Chunks [rem `append` bytes]) >>== loop
            Just ch -> step (rem `append` bytes `snoc` 13 `snoc` ch) k
            Nothing -> do
              let remain = rem `append` bytes
              if null remain then yield (Continue k) EOF else k (Chunks [remain]) >>== loop

runMemcache :: MonadControlIO m => HostName -> Int -> Memcache m a -> m a
runMemcache host port act = bracket newConn (liftIO . flip withMVar hClose) (runReaderT act)
  where
    newConn = liftIO $ do
      h <- connectTo host $ PortNumber $ fromIntegral port
      newMVar h

instance (MonadControlIO m) => KVSBackend (Memcache m) where
  type CASUnique (Memcache m) = Int64
  get key = do
    write $ unwords ["get", key]
    dats <- memcache valueIter
    return $ liftM valValue $ listToMaybe dats
  gets key = do
    write $ unwords ["gets", key]
    dats <- memcache valueIter
    case listToMaybe dats of
      Just dat -> return $ flip (,) (valValue dat) <$> valUniq dat
      _        -> return Nothing
  delete key = do
    write $ unwords ["delete", key]
    memcache deleteIter
  set     = simpleStorage "set"
  replace = simpleStorage "replace"
  add     = simpleStorage "add"
  cas key val un  = storage "cas" key 0 0 (Just un) val

simpleStorage :: MonadControlIO m
              => ByteString
              -> ByteString
              -> ByteString
              -> Memcache m Bool
simpleStorage cmd key val = storage cmd key 0 0 Nothing val

deleteIter :: Monad m => Iteratee ByteString m Bool
deleteIter = do
  line <- LE.head
  case line of
    Just "DELETED"   -> return True
    Just "NOT_FOUND" -> return False
    _                -> fail "Unknown error"

storage :: MonadControlIO m
        => ByteString  -- ^ sotrage command name
        -> ByteString  -- ^ Key
        -> Int16       -- ^ Flags
        -> Integer     -- ^ Exptime
        -> Maybe Int64 -- ^ CAS Unique
        -> ByteString  -- ^ Data block
        -> Memcache m Bool
storage cmd key flag exptime uniq dat = do
  let len = show $ length dat
  write $ unwords $ [cmd, key, show flag, show exptime, len] ++ maybeToList (show <$> uniq)
  write dat
  memcache storeIter

(=$) :: Monad m => Enumeratee ao ai m b -> Iteratee ai m b -> Iteratee ao m b
(=$) = (joinI .) . ($$)

data Command = Storage { _command :: ByteString
                       , key      :: ByteString
                       , flag     :: Int16
                       , expTime  :: Integer
                       , cmdUniq  :: Maybe Int64
                       , cmdBytes :: ByteString
                       , noreply  :: Bool
                       }
             | Retrieval { _command :: ByteString
                         , keys     :: [ByteString]
                         , noreply  :: Bool
                         }
             | Delete { key     :: ByteString
                      , noreply :: Bool
                      }
             | OtherCommand { _command  :: ByteString
                            , otherArgs :: [ByteString]
                            , noreply   :: Bool
                            }
               deriving (Show, Eq, Ord, Data, Typeable)

command :: Command -> ByteString
command Delete{} = "delete"
command cmd      = _command cmd

data Value = Value { valKey :: ByteString
                   , valValue :: ByteString
                   , valUniq :: Maybe Int64 }
           deriving (Show, Eq, Ord)

memcache :: MonadControlIO m => Iteratee ByteString IO a -> Memcache m a
memcache iter = do
  conn <- ask
  liftIO $ withMVar conn $ \h -> run_ (BE.enumHandle 100 h $$ enumLine =$ iter)

valueIter :: MonadIO m => Iteratee ByteString m [Value]
valueIter = do
  line <- LE.head
  case words <$> line of
    Nothing      -> returnI $ Error $ SomeException $ ProtocolError "END was expected but not."
    Just ["END"] -> return  []
    Just ("VALUE":key:flags:bytes:cas) -> do
      dat <- toStrict <$> BE.take (read $ unpack bytes)
      (Value key dat (read.unpack <$> listToMaybe cas):) <$> valueIter

storeIter :: MonadIO m => Iteratee ByteString m Bool
storeIter = do
  line <- LE.head
  case line of
    Just "STORED" -> return True
    _             -> return False    

write :: MonadIO m => ByteString -> Memcache m ()
write str = ReaderT $ \mh -> do
  liftIO $ withMVar mh $ \h -> hPut h str >> hPut h "\r\n" >> hFlush h
  
recv :: MonadIO m => Memcache m ByteString
recv = ReaderT $ \mh -> liftIO (withMVar mh $ \h -> hGetLine h)
