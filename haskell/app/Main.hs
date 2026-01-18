{-# LANGUAGE BangPatterns #-}

module Main (main) where
import qualified Data.HashMap.Strict as M
import Data.List (foldl')
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Char (digitToInt)

main :: IO ()
main = do
    contents <- B.readFile "../data/measurements.txt"  
    let res = foldl' update M.empty (B.lines contents)
    print $ M.lookup (B.pack "Bytom") res

update ::M.HashMap B.ByteString (Int, Int, Int, Int) -> B.ByteString -> M.HashMap B.ByteString (Int, Int, Int, Int)
update !accMap !line = 
    let 
        (city,rest) = B.break (==';') line
        temp = parseTemp rest
    in M.insertWith aggregate city (temp,temp,temp,1) accMap

parseTemp :: B.ByteString -> Int
parseTemp = parse 0 
    where 
        parse acc bs 
            | B.null bs = acc
            | h == '-' = -parse acc t 
            | h == '.' || h ==';' = parse acc t
            | otherwise = parse (10*acc + digitToInt h) t
            where
                h = B.head bs
                t = B.tail bs

aggregate :: (Ord a, Ord b, Num c, Num d) => (a, b, c, d) -> (a, b, c, d) -> (a, b, c, d)
aggregate (oldMin,oldMax,oldSum,oldCount) (newMin,newMax,newSum,newCount) = (min oldMin newMin,max oldMax newMax, oldSum + newSum,oldCount+newCount)


