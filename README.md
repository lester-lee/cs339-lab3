# cs339-lab3
I wrote the map function & imported json libraries! Copied Jeannie's Makefile
Hadoop uses a few classes but it's all up on the API (the link is on the assignment page)
They're like wrappers so you have to initiate them & then set their value
After that, to output a pair you do: `context.write(key, value)`
Jeannie's trivial example is pretty helpful, but if you look up a Hadoop wordcount example that's super useful because it tells you how to read in a file/key-value pairs and output more stuff
Still need to:
Write the reduce functions
Test them single-node (ask Jeannie for help?)   
Move code to cluster & test there
I don't think our output key/value pairs are completely correct, cause maybe both impression&click ones should have the same key? Right now they're different (look at FrequencyReducer). I started writing the FrequencyReduce key so you have an idea of how reduce works: it looks at one key, and has an iterator of all the values that the mapper outputted. Try figuring out what the correct pair to output for FrequencyReducer is so that the next Reducer will be able to reduce the pairs into our final <[Referrer,AdId], Clickrate> pairs that we want!
OH BTW YOU WILL NEED TO DOWNLOAD THE CLICKS_MERGED and IMPRESSIONS_MERGED INTO YOUR FOLDERS!! AND ALSO DATAKEY.PEM cause they're like really big lol & I can't upload them to github   lmk if any of my code doesn't make sense