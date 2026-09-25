# CS324-A1

Java RMI cluster that runs distributed jobs across worker nodes under an elected coordinator.

## Prerequisites

- Java 17
- Maven 3.9+

## Build

```bat
mvn package -DskipTests
```

## Run

Use a separate terminal for each process.

1. Bootstrap node (directory service, default port 1099):

```bat
java -cp target\classes com.cs324a1.bootstrap.BootstrapNode 1099
```

2. Workers (each needs a unique integer ID):

```bat
java -cp target\classes com.cs324a1.worker.WorkerNode 1 localhost 1099
java -cp target\classes com.cs324a1.worker.WorkerNode 2 localhost 1099
java -cp target\classes com.cs324a1.worker.WorkerNode 3 localhost 1099
```

New workers register with the bootstrap and connect to one random active worker. Leave them running.

3. Client GUI (run one or more instances):

```bat
java -cp target\classes com.cs324a1.client.ClientApp
```

## Using the client

- Set Host, Port, and Client ID in the CONNECTION section. The header shows live worker count.
- In NEW JOB, pick an operation:
  - `MAX` — largest value in a list
  - `PRIMECOUNT` — count of primes in a list
  - `PRIMESUM` — sum of primes in a range
- Enter data manually (`2, 4, 5, 11` or `start` / `end`) or use Load CSV.
- Submit Job. Multiple jobs can run concurrently.

## CSV format

List jobs (`MAX`, `PRIMECOUNT`): single column with optional header, one integer per line:

```csv
value
83811
14593
3279
```

Range jobs (`PRIMESUM`): single row with optional header:

```csv
start,end
1,1000
```

Sample files in `data/`:

- `numbers-sm.csv` with 100 entries
- `numbers-md.csv` with 500 entries
- `numbers-lg.csv` with 1000 entries

## Tests

```bat
mvn test
```

## Authors

Vishant Kumar: S11230430
Anav Chand: S11221203
James Kado: S11200776
Rohan Nandan: S11234883
