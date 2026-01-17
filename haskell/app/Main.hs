module Main (main) where
import qualified Data.Map as M

newtype Record = Record { getRecord :: (Int,Int,Int,Int) }

main :: IO ()
main = do
    contents <- readFile "../data/measurements.txt"  
    let map = M.fromList ([] :: [(String,Record)])
    let n = length . lines $ contents
    putStrLn $ "Lines: " ++ show n




