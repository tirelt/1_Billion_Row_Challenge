module Main (main) where
import qualified Data.Map as M
import Control.Monad
import Data.List

newtype Record = Record { getRecord :: (Int,Int,Int,Int) }

main :: IO ()
main = do
    contents <- readFile "../data/measurements.txt"  
    --let res = foldl update M.empty (lines contents)
    foldM_ printAndUpdate M.empty (lines contents)

printAndUpdate acc line = do
    let acc' = update acc line
    print acc'
    return acc'

update :: M.Map [Char] Int -> [Char] -> M.Map [Char] Int
update map line = M.insert city (newVal exist) map
    where 
        (city,temp) = parse line
        exist = M.lookup city map
        newVal (Just n) = n + 1
        newVal _ = 1


parse line = let (city,_:tempString) = break (==';') line in (city, read tempString :: Float)
