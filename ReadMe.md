# ReadMe
## This is a map-reduce program that will perform EquiJoin.
• The code is in Java using Hadoop Framework.

• The code takes two inputs, one is the hdfs location of the file on which the
equijoin should be performed and other is the hdfs location of the file, where the output should be stored.

### To Test the code: 

1) Copy the program code in your IDE project workspace. 

2) Add the necessary Hadoop dependencies in the project before running the program.

3) Give the paths for Input and Output as command line arguments 

-----------------------------------------------------------------------------------
### Approach taken :

1) In Map phase, the input is read from the hdfs file, line by line and  sets the key value pairs. 
Here, the key is Joining Key and the value has the relation name and the tuple.

2) The Reduce phase separates  R tuples and Relation S tuples based on relation name.
Then based on joining key,if key matches, the tuples of S are added to tuples of R.
The final output of EquiJoin is written in output hdfs file.
