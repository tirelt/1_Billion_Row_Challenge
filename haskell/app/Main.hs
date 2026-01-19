{-# LANGUAGE BangPatterns #-}

module Main (main) where
import qualified Data.HashMap.Strict as M
import Data.List (foldl')
import qualified Data.ByteString.Char8      as B   
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Char (digitToInt)
import Control.Concurrent
import Data.Time.Clock
import Control.Monad (replicateM_,replicateM, forM)

main :: IO ()
main = do
    contents <- BL.readFile "../data/measurements.txt"  
    let linesBL = BL.lines contents
    let numLines = 1000000
    let numWorkers = 3
    chunkChan <- newChan
    resChan <- newChan
    forkIO $ readerThread linesBL numLines numWorkers chunkChan 0 
    replicateM_ numWorkers $ forkIO $ workerThread chunkChan resChan M.empty 0 0 1
    results <- replicateM numWorkers $ readChan resChan
    print "finished"
    let finalRes = foldl1 (M.unionWith aggregate) results
    --print results
    return ()

readerThread :: [BL.ByteString] -> Int -> Int -> Chan [B.ByteString]-> Int -> IO ()
readerThread [] _ numWorkers chunkChan _ = replicateM_ numWorkers $ writeChan chunkChan [] 
readerThread linesBL numLines numWorkers chunkChan numChunk = do 
    let !loaded = map BL.toStrict $ take numLines linesBL
    writeChan chunkChan loaded 
    readerThread (drop numLines linesBL) numLines numWorkers chunkChan (numChunk +1)

--workerThread :: Chan [B.ByteString] -> Chan (M.HashMap B.ByteString (Int,Int,Int,Int)) -> M.HashMap B.ByteString (Int, Int, Int, Int) -> -> IO ()
workerThread chunkChan resChan accMap waitTotal workTotal numCall = do
    waitStart <- getCurrentTime
    chunk <- readChan chunkChan
    waitEnd <- getCurrentTime
    let !newWait = diffUTCTime waitEnd waitStart
        !waitSum = waitTotal + newWait
    if null chunk then do
        putStrLn $ "Worker done | total wait: " ++ show waitSum  ++ " | total work: " ++ show workTotal ++ " | num call: " ++ show numCall 
        writeChan resChan accMap
    else do 
        processStart <- getCurrentTime
        let !m = foldl' update accMap chunk
        processEnd <- getCurrentTime
        let !newWork = diffUTCTime processEnd processStart
            !workSum = workTotal + newWork
        workerThread chunkChan resChan m waitSum workSum (numCall +1)

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


