Storm Hackathon
===============

Notes taken from http://plaflamme.github.io/storm-hackathon/start/

Prereqs
-------

1. git
2. [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
3. [Maven](http://maven.apache.org/download.cgi)
4. A fork/clone of https://github.com/mdawaffe/storm-hackathon/

To install the JDK on OS X, just type the following in a terminal, and you'll get a dialog prompting you to install.

~~~bash
$ java -version
~~~

Maven can be installed via brew or macports.

Setting up a Storm Cluster
--------------------------

You can run things locally, or on a VM.  The VM method was easier for me to set up.

### Local

I don't actually remember how to run things locally :)

If you want to do that, https://github.com/nathanmarz/storm-starter/ has some instructions, so it might be easier to follow that repo.

### VM

1. Install [VirtualBox](https://www.virtualbox.org/wiki/Downloads)
2. Install [Vagrant](http://docs.vagrantup.com/v2/installation/)
3. Initialize the VM:

   ~~~bash
   $ mkdir -p ~/VMs/storm
   $ cd ~/VMs/storm
   $ vagrant init storm https://dl.dropboxusercontent.com/u/2759041/Storm/stormvm.box
   ~~~

4. Configure the network in `~/VMs/storm/Vagrantfile`

   ~~~
   config.vm.network :private_network, ip: "192.168.101.11"
   ~~~

5. Start the VM

   ~~~bash
   $ vagrant up
   ~~~

6. Open the storm UI: http://192.168.101.11:8082
7. Configure storm to submit to the cluster on the VM

   ~~~bash
   $ mkdir -p ~/.storm
   $ echo 'nimbus.host: "192.168.101.11"' > ~/.storm/storm.yaml
   ~~~


Running a Storm Topology
------------------------

### Build
~~~bash
$ mvn package
~~~

(My maven command is actually `mvn3`.)

### Submit
~~~bash
$ ./tools/submit CLASS NAME
# ./tools/submit me.mdawaffe.storm.TrendingWordCountTopology Trendy
~~~

You can now see the topology running via the Storm UI: http://192.168.101.11:8082

To watch it, you need to log into the vagrant box and tail the logs:

~~~bash
$ cd ~/VMs/storm
$ vagrant ssh
# ... wait for it ...
$ tail -Fq /opt/storm/logs/worker-*.log
~~~

The logs can be pretty verbose.

### Kill
~~~bash
$ ./tools/kill NAME
# ./tools/kill Trendy
~~~
