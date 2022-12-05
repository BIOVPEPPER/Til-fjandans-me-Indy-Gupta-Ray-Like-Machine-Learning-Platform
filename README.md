# CS425_MP4_LiYao


## Join IDunno
At machine 3 to 10, under the repo folder, execute command
```
./commands.sh join
```
to join IDunno. You will need superuser privilege to for this.

Only when at VM1, execute command
```
python3 Coordinator.py &
python3 CoordinatorML.py &
```
Since VM1 is the default Coordinator.

Only when at VM2, execute command
```
python3 StandByCoordinator.py &
```
Since VM2 is the stand-by Coordinator.

## Submit job to IDunno 
At any machine that have joined IDunno, execute command
```
python3 tools.py IDunnoSUB job_type sdfsfilename batch_size
```
Where `job_type` is either 'V' or 'T', standing for 'vision' and 'tranlation',
`sdfsfilename` is the data to be inferenced
`batch_size` is the batch size.


## Start training
At any machine that have joined IDunno, execute command
```
python3 tools.py IDunnoMSG STR
```


## Start inferencing
At any machine that have joined IDunno, execute command
```
python3 tools.py IDunnoMSG SINF
```

## End jobs and collect result
At any machine that have joined IDunno, execute command
```
python3 tools.py IDunnoMSG EJ
```

## Commands
At any machine that have joined IDunno,  for commands C1, C2, C4, C5, execute command,
```
./commands.sh Cn
```
Where `n` is in {1,2,4,5}.

For C3, execute command,
```
./commands.sh C3 JobID batch_size
```
Where `JobID` the ID of Job you want to set batch_size of,
`batch_size` is the batch size.