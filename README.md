# OmniSketch
OmniSketch code for VLDB2024 Submission

Requirements:
  - maven

Java version:
- OpenJDK version 19.0.2

The following paths need to be changed:
  - Main.readFolder
  - Main.outputFolder

Arguments:
1. setting, one of ["Compare Baselines", "Compare Estimators", "Scalability", "Compare Distributions", "Range Queries"].
    - Note: for Compare Distributions, also set Main.sensitivityAnalysis = true;
2. dataset, one of ["CAIDA", "SNMP"] // In setting "Compare Distributions", synthetic datasets zipf1, zipf1-3, zipf1-5 and uniform are created.
3. repetition, number to assign repetition. Changing this number changes the hash functions. Example: "1".

Example command for jar file:
java -jar Omnisketch.jar "Compare Baselines" "SNMP" "1"
will run OmniSketch and Hydra on SNMP data for repetition 1.


Datasets:
- CAIDA. Can be downloaded via https://www.caida.org/catalog/datasets/passive_dataset/. Use equinix-chicago.
    - The dataset is split into separate files. In experiments that vary the stream size of CAIDA, one should provide numbers of files to run. An example can be           found in Scalability.java, line 119. Here, we ran ["1", "5", "10", "15","20"], which determined how many files we used. 20 corresponds to the largest               dataset of 109M records.
- SNMP: Available via https://ieee-dataport.org/open-access/crawdad-dartmouthcampus-v-2004-08-05. Use fall03.tar.gz.
    - This dataset is split into different folders with names as "031101". For experiments that vary stream size of SNMP, one should use different conditions that         the folder names should satisfy. An example can be found in compare estimators at line 162. Here, we run the dataset for conditions ["03110","03110_OR_03111", "0311", "03", "0"]. The condition "0" belongs to the largest dataset.
- Synthetic: Can be generated by running Compare Distributions. It is possible to set parameters at the start of Dataset.java.
  
Queries:
- Generate point queries by setting Main.createNewWorkload
- Generate range queries by setting Main.createNewRangeWorkload

After creating a new workload, check the queries and look which attribute combinations are most frequent. This is important for experiments that check for different numbers of stored attributes, see line 75 in CompareBaselines.java. Every iteration, we want to add an attribute that brings new queries combined with the existing attributes. For our workload, the right order of attributes was [12, 14, 11, 1, 5, 13, 9, 8, 10, 21, 15], because 12&14 had the most queries.

For original Hydra code, see https://github.com/antonis-m/HYDRA_VLDB
