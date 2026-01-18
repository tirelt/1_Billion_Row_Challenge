module Main (main) where
import qualified Data.Map as M
import Control.Monad
import Data.List

main :: IO ()
main = do
    contents <- readFile "../data/measurements.txt"  
    let res = foldl' update M.empty (lines contents)
    print $ M.lookup "Bytom" res

update map line = 
    let 
        (city,_:tempString) = break (==';') line
        temp = read tempString :: Float
    in M.insertWith aggregate city (temp,temp,temp,1) map

aggregate (oldMin,oldMax,oldSum,oldCount) (newMin,newMax,newSum,newCount) = (min oldMin newMin,max oldMax newMax, oldSum + newSum,oldCount+newCount)


