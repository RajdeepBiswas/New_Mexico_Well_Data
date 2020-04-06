# New_Mexico_Well_Data
Production data from New Mexico is published each month in a zipped, 36 GB XML file.  It contains data for 55,000 wells over the past 30 years.  The file grows in size by 300 MB per month. This repository uses spark code to process the file.

# Design Considerations
The file is parsed and stored in smaller (consumable) files of csv format. We have a more than 38 million rows from 1970s to 2020.

This file had some unique challenges which I wanted to share:
1.	The XML file did not have any unix or windows style line endings. For a processing engine like spark for example it means we are asking to process a string which is of size 36 GB.
2.	Pertaining to point 1 above, one obvious solution can be using a custom line ending. Well it turns out to parse a custom line ending the spark engine will try to read at least one line. In our case it is of size 36GB and it is going to fail since it exceeds the max line size.
3.	Obviously based on point 1 and 2 any kind batch processing will fail (I tried with machines having 256GB RAM, but it did not work)
4.	Hence, I had to use streaming. Now, stream processing uses bytecode to read the files hence multiple times I had to decode the file to get rid of special encoding characters. For example, the streaming will automatically decode it as UTF16 to read the large chunks.
I also made it a point to not use custom readers until all options are exhausted. Hence, I had to switch between streaming, batching, spark sql, data frames and make the best use of available methods..


# Given below is the high level flow:
1. Create Azure databricks mounts
2. Download and unzip the large file to Azure Blob Storage
3.	Use streaming to read the file in small chunks and parse the new lines
4.	Write the streaming results 
5.	Run batch processing (transformations/decoding etc) on the results of the streaming output files
6.	Write it to the final destination.
