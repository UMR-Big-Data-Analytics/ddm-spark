# ddm-spark
Spark example and homework code for the "Distributed Data Management" lecture.

## Setup instructions

This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit.

### Checkout the project
**git clone git@github.com:UMR-Big-Data-Analytics/ddm-spark.git**

### Install IntelliJ
- Select the Scala and sbt plugins to be installed during the installation; otherwise you need to install them later

### Import the project into IntelliJ
- Start Intellij
- Select **Open or Import**
- Select the **ddm-spark/build.sbt** file and click **OK**
- Click **Open as Project** (IntelliJ should automatically recognize the sbt project nature; note that the initialization might take a while for missing downloads and indexing; if IntelliJ does not automatically download the depencencies, you can open the sbt tab on the right edge of the screen and click **Reload all sbt projects**)

### Load the homework data
- Download the tpch.zip from https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2017/DDA/TPCH.zip
- Extract the zipped content into **ddm-spark/data**, which should create the folder **ddm-spark/data/TPCH** with 7 csv files

### Run the tutorial
- Switch to your IntelliJ window and open **ddm-spark/src/main/scala/de/ddm** in the project tree view
- Right-click **DDMSpark.scala** and click **Run DDMSpark**

### Solve the DDM homework
- Remove the Tutorial and LongestCommonSubstring calls
- Implement the Sindy algorithm

### Build your final jar file
- Click **Run**->**Edit Configurations**->**+**->**SBT Task** 
- Enter a name, such as **DDMSpark assembly**
- Enter task **clean assembly**
- Click **OK**
- You can now switch to and run the **DDMSpark assembly** target in the top right Intellij bar
- Find your fat-jar in **ddm-spark/target/scala-2.12/DDMSpark-assembly-0.1.jar**

### Some further notes for cluster submits
- Modify the settings in the build.sbt to match the spark installation (Relevant settings are both the **scalaVersion** parameter and the specific versions of all the spark packages (for example in **libraryDependencies += **org.apache.spark** %% **spark-core** % **x**** replace x with the version of the spark installation, where you want to execute the code). If the scala version and the spark version do not exactly match those of the spark installation, you will encounter errors while executing the jar that are not really helpful in determining the cause.
