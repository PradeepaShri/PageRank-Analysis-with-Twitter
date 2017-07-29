# Twitter PageRank Algorithm with Hadoop using Kotlin

Kotlin Implementation of the PageRank algorithm and analysis of twitter dataset with Hadoop using Amazon EC2. 

#### PageRank

This program takes an input file which has graph data in the form of node and its adjacency lists and generates files containing nodes, their page ranks and their adjacency lists. It also generates a file which has the top ten nodes listed in descending order of their page ranks and other graph data.

## Features

- Used Map Reduce framework for faster processing
- Used FileSystem API for cleaning up intermediate result files

## PageRank

The PageRank algorithm is given by

`PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))`

where

	PR(A) is the PageRank of page A,
	PR(Ti) is the PageRank of pages Ti which link to page A,
	C(Ti) is the number of outbound links on page Ti and
	d is a damping factor which can be set between 0 and 1.

## Pseudocode

```
procedure PageRank(G, iteration)             ◃ G: inlink file, iteration: # of iteration
    d ← 0.85                                                      ◃ damping factor: 0.85    oh ← G                                               ◃ get outlink count hash from G
    ih ← G                                                      ◃ get inlink hash from G
    N ← G    for all p in the graph do        opg[p] ← 1/N                                               ◃ initialize PageRank    end for    while iteration > 0 do        dp ← 0        for all p that has no out-links do            dp ← dp + d ∗ opg[p]/N           ◃ get PageRank from pages without out-links        end for        for all p in the graph do            npg[p] ← dp + 1−d/N                          ◃ get PageRank from random jump            for all ip in ih[p] do                npg[p] ← npg[p] + d∗opg[ip]/oh[ip]           ◃ get PageRank from inlinks            end for 
        end for        opg ← npg                                                      ◃ update PageRank        iteration ← iteration − 1                       
    end whileend procedure
```

## File Description

![fig_1_10](http://i.imgur.com/foxqpb7.jpg)

#### PageRank.kt

- Driver class.
- Main class in which the iterations of map and reduce phase start.
- Fixes a desired convergence, initializes the initial page rank to a value = (1/no of nodes in the graph)
- Reads the input file and creates an intermediary input file for the first Map task.
- In the iterative loop, the map and reduce tasks are defined and their input and output paths provided.
- At the end of each iteration, calculate the convergence based on sum of convergence, no. of nodes and convergence scaling factor.
- If the convergence is less than the desired convergence stop the iterations.
    - Else set the input path for the next map phase as the output path of the current reduce phase.
- After the iterations are over, the final output part files created by the reducer are read and sorted to list the top ten nodes with the highest page rank and other graph data such as no. of nodes, no. of edges etc is written to new file.

#### PageRankMapper.kt

- Mapper class. 
- Receives a nodeid as key and node information as value
- Emits the nodeid and the node information first.
- Calculates pagerank for each of the adjacent nodes.
- For each of the nodes in its adjacency list
	 - It emits a key value pair of (nodeid, pagerank)
	
#### PageRankReducer.kt

- Reducer class. It receives a key- nodeid and a list of values.
- For each value in the list
	 - If it is a node, the initialize the node with this node.
        - Else if it a pagerank value sum it up
- Sets the pagerank in the initialized node.
- Finally emits a key value pair of (nodeid, node)

## Hadoop

![1](http://i.imgur.com/cWhqtlH.png)



