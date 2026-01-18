{-# LANGUAGE BangPatterns #-}

module Main (main) where
import qualified Data.Map.Strict as M
import Control.Monad
import Data.List
import qualified Data.ByteString.Lazy.Char8 as B

main :: IO ()
main = do
    contents <- B.readFile "../data/measurements.txt"  
    let res = foldl' update M.empty (B.lines contents)
    print $ M.lookup (B.pack "Bytom") res

update ::M.Map B.ByteString (Float, Float, Float, Int) -> B.ByteString -> M.Map B.ByteString (Float, Float, Float, Int)
update !map !line = 
    let 
        (city,rest) = B.break (==';') line
        temp = read (B.unpack (B.drop 1 rest)) :: Float
    in M.insertWith aggregate city (temp,temp,temp,1) map

aggregate (oldMin,oldMax,oldSum,oldCount) (newMin,newMax,newSum,newCount) = (min oldMin newMin,max oldMax newMax, oldSum + newSum,oldCount+newCount)


