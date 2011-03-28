{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
{-# LANGUAGE TypeFamilies, GeneralizedNewtypeDeriving #-}
module Main where
import Yesod
import Database.Persist.TH
import Database.Persist.GenericKVS ()
import Database.Persist
import Data.Text (Text)
import Database.Persist.Memcached (runMemcachedPersist)
import Data.Time
import Data.Int

share [mkPersist] [persist|
User
  name String Eq
  age Int
  email String Eq
  UniqueUser name
Article
  user UserId Eq
  title Text Eq Update
  text  Text Update
  createdAt UTCTime Gt Lt Eq default=CURRENT_TIME
  UniqueArticle title createdAt
|]

main :: IO ()
main = do
  runMemcachedPersist "127.0.0.1" 11212 $ do
    key   <- insert $ User "nueria18" 19 "konn.jinro@gmail.com"
    liftIO $ print key
    mHoge <- getBy $ UniqueUser "mr_konn"
    liftIO $ print mHoge
    time <- liftIO getCurrentTime
    k <- insert $ Article key "hello" "あーあー。てすとてすと。\nほげほげ。" time
    newtime <- liftIO getCurrentTime
    replace k $ Article key "dummy" "this article was jacked :-p" newtime
    getBy (UniqueArticle "hello" time) >>= liftIO . print
    getBy (UniqueArticle "dummy" newtime) >>= liftIO . print
