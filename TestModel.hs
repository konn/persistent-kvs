{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
{-# LANGUAGE TypeFamilies, GeneralizedNewtypeDeriving #-}
module Main where
import Yesod
import Database.Persist.TH
import Database.Persist.GenericKVS ()
import Database.Persist
import Data.Text
import Database.Persist.Memcached (runMemcachedPersist)
import Data.Time
import Data.Int

share2 mkPersist (const $ return []) [persist|
User
  name String Eq
  age Int
  email String Eq
  UniqueUser name
Article
  user UserId Eq
  title Text Eq
  text  Text
  createdAt UTCTime Gt Lt Eq
  UniqueArticle title createdAt
|]

main :: IO ()
main = do
  runMemcachedPersist "127.0.0.1" 11212 $ do
    key   <- insert $ User "mr_konn" 19 "konn.jinro@gmail.com"
    mHoge <- getBy $ UniqueUser "mr_konn"
    liftIO $ print mHoge
