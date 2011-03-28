{-# LANGUAGE TypeSynonymInstances, OverloadedStrings #-}
module Main where
import Test.QuickCheck
import Database.Persist.Memcached
import Data.Attoparsec
import Blaze.ByteString.Builder
import Control.Applicative
import Database.Persist.Base
import Data.ByteString
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time
import Data.Fixed
import Data.Ratio
import Data.Maybe
import Prelude hiding (length)
import Data.Monoid
import Data.Word
import System.Random

main :: IO ()
main = do
  quickCheck prop_doubleSound
  quickCheck prop_picoSound
  quickCheck prop_integerSound
  quickCheck prop_persistSound
  quickCheck prop_persistInvSound

prop_persistSound :: PersistValue -> Bool
prop_persistSound v = comp (anyPersistValue) buildPersistValue v == Just v

prop_picoSound :: Pico -> Bool
prop_picoSound v = comp anyPico fromResolution v == Just v

prop_integerSound :: Property
prop_integerSound = forAll (choose (0,2^35)) $ \v -> comp anyInteger buildInteger v == Just v

prop_doubleSound :: Double -> Bool
prop_doubleSound v = comp anyDouble fromDouble v == Just v

prop_persistInvSound :: Property
prop_persistInvSound = forAll arbitraryCode $ \bs -> 
  let r = maybeResult $ parse (anyPersistValue) bs
  in isJust r ==> maybe "" (toByteString . buildPersistValue) r == bs

comp :: Parser a -> (a -> Builder) -> a -> Maybe a
comp p b v = maybeResult $ flip feed "" $ parse p $ toByteString $ b v

instance Arbitrary PersistValue where
  arbitrary = do
    num <- choose (0,8) :: Gen Int
    case num of
      0 -> PersistString <$> arbitrary
      1 -> PersistByteString . T.encodeUtf8 . T.pack <$> arbitrary
      2 -> PersistInt64 <$> arbitrary
      3 -> PersistDouble <$> arbitrary
      4 -> PersistBool <$> arbitrary
      5 -> PersistDay <$> arbitraryDay 
      6 -> PersistTimeOfDay <$> (TimeOfDay <$> arbitrary <*> arbitrary <*> arbitraryPico)
      7 -> PersistUTCTime <$> (UTCTime <$> arbitraryDay <*> arbitraryDiffTime)
      8 -> pure PersistNull

arbitraryPico :: Gen Pico
arbitraryPico = toEnum <$> arbitrary

arbitraryDay :: Gen Day
arbitraryDay = ModifiedJulianDay <$> arbitrary

arbitraryDiffTime :: Gen DiffTime
arbitraryDiffTime = toEnum <$> arbitrary

instance Arbitrary Pico where
  arbitrary = arbitraryPico

instance Arbitrary ByteString where
  arbitrary = T.encodeUtf8 . T.pack <$> listOf arbitrary

arbitraryCode :: Gen ByteString
arbitraryCode = do
  tag <- choose (0::Int, 2)
  case tag of
    0 -> do
      str <- arbitrary
      let len = length str
      return $ toByteString $ fromWord8 0 `mappend` fromWord64le (fromIntegral len) `mappend` fromByteString str
    1 -> do
      str <- arbitrary
      let len = length str
      return $ toByteString $ fromWord8 1 `mappend` fromWord64le (fromIntegral len) `mappend` fromByteString str
    2 -> do
      i64 <- arbitrary
      return $ toByteString $ fromWord8 2 `mappend` fromInt64le i64
    3 -> do
      i <- arbitrary
      b <- arbitrary :: Gen Int
      return $ toByteString $ fromWord8 3 `mappend` buildInteger i `mappend` fromInt64le (fromIntegral b)
    4 -> do
      b <- choose (0,1)
      return $ toByteString $ fromWord8 4 `mappend` fromWord8 (fromInteger b)
    5 -> do
      a <- arbitrary
      return $ toByteString $ fromWord8 5 `mappend` buildInteger a 
    6 -> do
      a <- arbitrary
      b <- arbitrary
      c <- arbitraryPico
      return $ toByteString $ fromWrite (mconcat [writeWord8 6, writeWord64le a, writeWord64le b])
                                `mappend` fromResolution c
    7 -> do
      a <- arbitrary
      b <- arbitrary
      return $ toByteString $ fromWord8 7 `mappend` buildInteger a `mappend` fromInt64le b
    8 -> return $ toByteString $ fromWord8 8
