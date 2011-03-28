{-# LANGUAGE OverloadedStrings #-}
module Database.Persist.KVS.Internal
  ( toValue
  , fromValue
  , encodePersistValue
  , decodePersistValue
  , encodeValues
  , decodeValues
  , escape
  , unescape
  ) where
import Network.HTTP.Types
import Data.ByteString hiding (take)
import Blaze.ByteString.Builder
import Control.Applicative
import Data.Fixed
import Data.Ratio
import Data.Attoparsec
import Data.Attoparsec.Binary
import Data.Bits
import Data.Monoid
import Data.Int
import Data.Word
import Database.Persist.Base
import Data.Time
import Control.Monad
import Prelude hiding (take, foldr, length)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Ascii as A
import Data.Maybe

toValue :: PersistField a => a -> ByteString
toValue = encodePersistValue . toPersistValue

fromValue :: PersistField a => ByteString -> Maybe a
fromValue = either (const Nothing) id . fromPersistValue <=< decodePersistValue

encodePersistValue :: PersistValue -> ByteString
encodePersistValue = toByteString . buildPersistValue

decodePersistValue :: ByteString -> Maybe PersistValue
decodePersistValue = maybeResult . parse anyPersistValue

buildPersistValue :: PersistValue -> Builder
buildPersistValue (PersistString str) =
  let bs = T.encodeUtf8 $ T.pack str
  in fromWrite $ writeWord8 0 `mappend` writeWord64le (fromIntegral $ length bs)
                              `mappend` writeByteString bs
buildPersistValue (PersistByteString bs) =
  fromWrite $ writeWord8 1 `mappend` writeWord64le (fromIntegral $ length bs)
                           `mappend` writeByteString bs
buildPersistValue (PersistInt64 i) =
  fromWrite $ writeWord8 2 `mappend` writeInt64le i
buildPersistValue (PersistDouble dbl) =
  fromWrite (writeWord8 3) `mappend` fromDouble dbl
buildPersistValue (PersistBool b) =
  fromWrite $ writeWord8 4 `mappend` if b then writeWord8 1 else writeWord8 0
buildPersistValue (PersistDay day) =
  fromWrite (writeWord8 5) `mappend` buildInteger (toModifiedJulianDay day)
buildPersistValue (PersistTimeOfDay (TimeOfDay a b c)) =
  mconcat [ fromWrite $ writeWord8 6
          , fromWrite $ writeInt64le (fromIntegral a)
          , fromWrite $ writeInt64le (fromIntegral b)
          , fromResolution c
          ]
buildPersistValue (PersistUTCTime (UTCTime a b)) =
  mconcat [ fromWrite $ writeWord8 7
          , buildInteger $ toModifiedJulianDay a
          , fromInt64le $ floor $ toRational $ b * 10^12
          ]
buildPersistValue PersistNull = fromWord8 8

fromDouble :: Double -> Builder
fromDouble dbl =  buildInteger a `mappend` fromInt64le (fromIntegral b)
  where (a, b) = decodeFloat dbl

fromResolution :: (RealFrac (p a), HasResolution a) => p a -> Builder
fromResolution a = buildInteger $ floor $ a * fromInteger (resolution a)

buildInteger :: Integer -> Builder
buildInteger n | lo <= n && n <= hi = fromWord8 0 `mappend` fromInt32le (fromIntegral n)
  where
    lo = fromIntegral (minBound :: Int32)
    hi = fromIntegral (maxBound :: Int32)
buildInteger n = fromWord8 1 `mappend` fromWord8 (fromIntegral $ signum n)
                             `mappend` fromInt64le (fromIntegral $ length bts)
                             `mappend` fromByteString bts
  where
    bts = unfoldr step $ abs n
    step 0 = Nothing
    step i = Just (fromIntegral i, i `shiftR` 8)

anyPersistValue :: Parser PersistValue
anyPersistValue = do
  b <- anyWord8
  case b of
    0 -> PersistString . T.unpack . T.decodeUtf8 <$> (take . fromIntegral =<< anyWord64le)
    1 -> PersistByteString <$> (take . fromIntegral =<< anyWord64le)
    2 -> PersistInt64 <$> anyInt64le
    3 -> PersistDouble <$> anyDouble
    4 -> PersistBool True  <$ word8 1
     <|> PersistBool False <$ word8 0
    5 -> PersistDay . ModifiedJulianDay <$> anyInteger
    6 -> PersistTimeOfDay <$> (TimeOfDay <$> (fromIntegral <$> anyInt64le)
                                         <*> (fromIntegral <$> anyInt64le)
                                         <*> anyPico)
    7 -> PersistUTCTime <$> (UTCTime <$> (ModifiedJulianDay <$> anyInteger)
                                     <*> ((/10^12). fromIntegral <$> anyInt64le))
    8 -> pure PersistNull
    _ -> fail $ "Unknown tag:" ++ show b

anyInt64le :: Parser Int64
anyInt64le = fromIntegral <$> anyWord64le

anyInt32le :: Parser Int32
anyInt32le = fromIntegral <$> anyWord32le

anyInteger :: Parser Integer
anyInteger = word8 0 *> (toInteger <$> anyInt32le)
         <|> word8 1 *> ((*) . toInteger <$> anyInt8 <*> step)
  where
    step :: Parser Integer
    step = foldr (flip $ (. fromIntegral) . (.|.) . (`shiftL` 8)) 0 <$> unroll
    unroll :: Parser ByteString
    unroll = anyInt64le >>= take . fromIntegral

anyDouble :: Parser Double
anyDouble = encodeFloat <$> anyInteger <*> (fromIntegral <$> anyInt64le)

anyInt8 :: Parser Int8
anyInt8 = fromIntegral <$> anyWord8

anyPico :: Parser Pico
anyPico = fromRational . (%10^12) <$> anyInteger

encodeValues :: [PersistValue] -> ByteString
encodeValues = intercalate ":" . Prelude.map (escape . encodePersistValue)

decodeValues :: ByteString -> [PersistValue]
decodeValues = mapMaybe (decodePersistValue . unescape). split 58

escape, unescape :: ByteString -> ByteString
escape = A.toByteString . urlEncode True
unescape = urlDecode True
