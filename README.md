# Collation of EBI Plant Samples and associated data files into JSON for incorporation into FAIR Data-finder for Agronomic REsearch

### Introduction

As part of Elixir Work Package 7, Ensembl Plants have produced this project to collate EBI plant data into 2 JSON files to be incorporated into FAIDARE: "FAIR Data-finder for Agronomic REsearch" (https://urgi.versailles.inra.fr/faidare/). The participants of work package 7 are crop plant research institutes who implement the BrAPI breeding API on top of their crop experiment databases. FAIDARE regularly makes specific BrAPI calls to each member API to collect details of available germplasms and phenotyping studies and indexes these as part of the federated search so that crop experiments and metadata can be discovered from a single centralised point.

The EBI has many databases and services so it is not feasible to build a custom plant API on top of them. However EBI is an important resource for genetic experiments including NGS sequencing, genotyping/variant calling, reference assemblies and expression studies (RNA-Seq). Specialist EBI groups in these data types exist such as EVA, Expression Atlas and BioSamples but all the data files are archived at the ENA, who have powerful search and API services. So in this project ENA is used to find all plant data across the different services at the EBI and 2 JSON files are created. One, called gp_dump.json contains all the germplasms found. It corresponds to brapi/v1/germplasms BrAPI call. The second one is study_dump.json and this contains all studies found and corresponds to BrAPI call brapi/v1/studies. Each study has a list of germplasms used and each germplasm has a list of studies it belongs to so the files cross reference each other. 

The JSON files are placed on an ftp server so that FAIRDARE can access them. The contacts at FAIRDARE arranging this are:
Cyril Pommier <cyril.pommier@inra.fr>
Bilal El-Houdaigui <Bilal.El-Houdaigui@inra.fr>
Jeremy Destin <Jeremy.Destin@inra.fr>

### Customisations

ENA and Biosamples do not impose a mandatory germplasm field for plant sample submissions but the germplasm id is central to crop phenotyping experiments. To create the germplasm JSON each EBI sample is checked for a germplasm id using a regular expression on its main id fields. If found, a germplasm is created by extracting the guessed germplasm id from the sample metadata along with other (potentially informative) attributes. 

### Python packages
Version used: Python 3.5.1

#### packages used
* certifi==2018.4.16
* chardet==3.0.4
* idna==2.7
* ijson==2.3
* lxml==4.2.1
* requests==2.19.1
* urllib3==1.23

You do not have to install any packages if you use the original Python installation (EBI internal) for this project here:
/nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/python

Both Python scripts fillsamples.py and dumpsamples.py have the above installation in their 'shebang' line and thus can be run from the command line or via the shell script included without any additional action. 

For testing or running independently you can enter a virtual environment by executing the following
```bash
source /nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/activate
```


