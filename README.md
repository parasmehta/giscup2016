# GetisOrd Hotspot Detection

## Using the code
If you use this code, please cite our [paper](doc/giscup2016_final.pdf).

```
Spatio-Temporal Hotspot Computation on Apache Spark.
Paras Mehta, Christian Windolf, and Agnès Voisard. 
24th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems (ACM SIGSPATIAL 2016)
```

## FU Berlin submission

This project is an implementation for the [GIS Cup 2016](http://sigspatial2016.sigspatial.org/giscup2016/). It won the [third place](http://sigspatial2016.sigspatial.org/giscup2016/results) among all submissions.


## Build
The build process requires the *Scala Build Tool*, aka `sbt`.

### Build without tests (fast)
To build the project faster, leave out the tests:

```bash
# Takes about 1 minute
sbt 'set test in assembly := {}' assembly
```

### Build with tests (slow)
This can take more than ten minutes due to the tests.

```bash
# Takes up to ten minutes
sbt assembly
```

After compiling, the jar can be found at `target/scala_2.10/hotspots-assembly-1.0.jar`.


## Startup
The application does not require any additional information other than the parameters specified
on the [submission page](http://sigspatial2016.sigspatial.org/giscup2016/submit).

```bash
spark-submit --master {spark-master-url} --class edu.fuberlin.hotspots.Submission target/ \
hdfs://path/to/input hdfs://path/to/output {gridsize} {timespan in days}
```
The minimum supported value for grid size is `0.0005` degrees to reduce memory overhead (see below) and the timespan in days accepts only integers.


## How it works
Our solution tries to maximize parallelism by putting cells and their immediate neighbours into the same partition so that they end up on the same node.
Our tests on a small test cluster show that the majority of time is spent on rearranging the data to align data locality with spatiotemporal locality and thus, very little time is spent on computing the statistic once the data has been arranged.

### First Phase (from lines to cells)
From the lines in the csv file, cells are created. This is done in a *map-reduce*-fashion.
For each line, a tuple (X,Y,T) consisting of the coordinates/id of the cell is created.

```
...,dropoffTime,passengerCount,...,dropoffLongitude, dropoffLatitude,...
...,2015-07-08 19:20:21,2,...,-73.983688354492187,40.766708374023438,... => ((-73983,40766,189), 2)
```

The cell id is further "compressed" into a single integer, to reduce the network traffic.
This is the reason why the grid size cannot be below 0.0005. Otherwise we might end up with more than 2 billion cells.

The set of initial cells gets reduced to the final cells by adding up the `passengerCount` if they have the same cell id.

### Second phase (from cells to supercells)
Now we want to make sure that the cells and their neighbours end up on the same node.
For this, a cube of cells is grouped into one *supercell*. However, a cell inside this cube might have neighbours residing in other supercells if it lies on the boundary of the cube. To avoid this, the boundary cells are stored in multiple supercells (up to 8). This is equivalent to creating a buffer around each supercell.
Each cell is a member of one and only one *core* of a supercell, but a cell can be member of the *buffer or boundary area* of up to 8 supercells (in a 3-dimensional space).

```
+---------------------------------+
!          boundary               !
!  ____________________________   !
!b |                           |b !
!o |                           |o !
!u |       CORE                |u !
!n |        OF                 |n !
!d |     SUPERCELL             |d !
!a |                           |a !
!r |                           |r !
!y |                           |y !
!  |                           |  !
!  L___________________________|  !
!           boundary              !
+---------------------------------+
```

### Phase Three (From Supercells to ZScores)
The formula to compute the GetisOrd statistic can be simply applied on this supercell structure, allowing it to be computed independently on each node.
Although the actual task is done in this phase, it took only a few seconds on a small test cluster.
