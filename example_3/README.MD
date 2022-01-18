# PIG

1. From Azure portal copy cluster ssh address.
    Grupy zasobów  -> "name of the group" (ex bigdata) -> enter to the "HDInsight cluster" -> Properties -> Copy Secure Shell (SSH).

2. From local shell copy example_3 to cluster:
```console
    $ ssh sshuser@[ssh_address] "mkdir -p /home/sshuser/example_3"
    $ scp -r  [git repo path]* sshuser@[ssh_address]:/home/sshuser/example_3
```
**where:**<br/>
* sshuser - is default name created by the Azure</br>
* [ssh_address] - address from the first step.</br>
* [git repo path] - local git repository -> C:/Users/vdi-student/hadoop/example_3/

3. Login to the cluster:
```console
    $ ssh ssh_address
```
**where:**<br/>
* [ssh_address] - address from the first step.</br>

4. From cluster shell copy data.txt to hdfs:
```console
    $ hdfs dfs -copyFromLocal /home/sshuser/example_3/data.txt /user/data.txt
```
**where:**<br/>
* sshuser - is default name created by the Azure<br/>

5. Run script on cluster
```console
    $ pig -x mapreduce ./example_3/rating.pig
```
