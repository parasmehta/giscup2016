# GetisOrd hotspot detection
## FU Berlin submission

This project is an implementation for the [SIGSPATIAL 2016 Cup](http://sigspatial2016.sigspatial.org/giscup2016/)

## Build
The build process requires the *Scala Build Tool*, aka `sbt`.
```bash
# Takes up to ten minutes
sbt assembly
```

This can take more than ten minutes due to very slow tests.
To build the project faster, leave out the tests:
```bash
# Takes about 1 minute
sbt 'set test in assembly := {}' assembly
```
After compiling, the jar can be found at `target/scala_2.10/hotspots-assembly-1.0.jar`

## Startup
The application does not require any additional information than the parameter specified
on the [submission page](http://sigspatial2016.sigspatial.org/giscup2016/submit).
```bash
spark-submit --master {spark-master-url} --class edu.fuberlin.hotspots.Submission target/ \
hdfs://path/to/input hdfs://path/to/output {gridsize} {timespan in days}
```
The minimum supported value for the gridsize is `0.0005`, the timespan in days accepts only integers.

## How it works
Our solution tries to maximize parallelism and accepts, that there will be a memory overhead.
In the end, this solution spends most of the time with rearranging the data.
The actual GetisOrd statistics is calculated in a few seconds.

### First Phase (from lines to cells)
From the lines in the csv file, cells have to be created.
This is done in a *map-reduce*-style.
For all lines, a tuple consisting of the coordinates/id of the cell is created.
```
...,dropoffTime,passengerCount,...,dropoffLongitude, dropoffLatitude,...
...,2015-07-08 19:20:21,2,...,-73.983688354492187,40.766708374023438,... => ((-73983,40766,189), 2)
```
The cell id is further "compressed" into a single integer, to reduce later the network traffic.
This is also the reason, why the grid size cannot be below 0.0005. Then we might end up with more than 2 billion
cells.

The the set of initial cells gets reduced too the final cells by adding up the `passengerCount`
if they have the same cell id.

### Second phase (from cells to supercells)
Now we want to make sure, that and their neighbours end up on the same node.
Therefore, a cube of cells is grouped into one supercell.
Because all cells should have their neighbours inside, boundary cells are needed.
At the end, every cell should be member of one and only one core of a supercell.
But a cell can be member of the boundary area of up to 8 supercells (in a 3-dimensional space).
```
+-------------------------------+
!  boundary                     !
! ____________________________  !
!b|                           |b!
!o|                           |o!
!u|       CORE                |u!
!n|        OF                 |n!
!d|     SUPERCELL             |d!
!a|                           |a!
!r|                           |r!
!y|                           |y!
! |                           | !
! L___________________________| !
!  boundary                     !
+-------------------------------+
```

### Phase Three (From Supercells to ZScores)
The formula to compute the GetisOrd statistics can be simply applied on this supercells structure allowing to
compute it independently.
Although the actual task is done in this phase, it took only a few seconds on the test cluster.
