{-# LANGUAGE TypeFamilies #-}
module Database.Persist.GenericKVS where
import Control.Monad.IO.Control

class MonadControlIO m => KVSBackend m where
  type CASUnique m
  type KVSKey m
  type KVSValue m
  get :: KVSKey m -> m (Maybe (KVSValue m))
  gets :: KVSKey m -> m (Maybe (CASUnique m, KVSValue m))
  delete, replace, set, add :: KVSKey m -> KVSValue m -> m Bool
  cas :: KVSKey m -> KVSValue m -> CASUnique m -> m Bool
  add = set
  replace = set

