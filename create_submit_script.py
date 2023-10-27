import argparse
import os
import sys
import glob

#For debugging and printing statements
import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


#The following file contains the submision scripts for processing of the muon reconstruction and studying the detector performance
#There are three independent steps that should be run in a sequence to produce the final output:
#Each subsequent step needs to wait before the previous step is finished as the output and inputs are linked

#1] Running reconstruction (Reco_tf) on the HIT files producing ESD
#2] Running MuonTester on the ESD files to produce root ntuples 
#3] Running TreeReader on the root ntuples to produce histograms
#TODO: create a pilot short job for testing


# Step 1
def create_submit_script_for_athena(**kwargs):
    logging.info('Creating submit script with the following arguments: {}'.format(kwargs))

    #find input files
    logging.info('Retrieving input files from the following dir {}'.format(kwargs["input_hits_dir"]))

    #Write a function that uses glob and returns a list of files with the .pool.root extension
    input_files = glob.glob(kwargs["input_hits_dir"] + "/*.pool.root")

    #Open an empty file in write mode with a name using submit_script_name argument
    logging.info('Creating submit script with the following name: {}'.format(kwargs["submit_script_name"]))
    with open(kwargs["submit_script_name"], "w") as f:

        for file in input_files:
            logging.info('Found file: {}'.format(file))

            #Figure out the DSID and the run number. Expected name has the following form: /dcache/atlas/muonreco/mvozak/HITS/PgunDiMu_Pt10to100R3/group.det-muon.33831010.EXT0._000390.HITS.pool.root
            file_name = file.split("/")[-1]
            data_dsid = file_name.split(".")[4]
            print(data_dsid)
            data_dsid = data_dsid.split("_")[1]

            #Split the file into multiple chunks if specified
            nsteps = 1
            events_to_process = kwargs["n_events_per_file"]
            if kwargs["split_into_chunks"]:
                nsteps = int( kwargs["n_events_per_file"] / kwargs["chunk_size"] )
                events_to_process = kwargs["chunk_size"]


            #check that the length of the DSID is 6
            if len(data_dsid) != 6:
                logging.error("DSID is not 6 digits long")
                sys.exit(1)

            #Loop over the number of events and create a job for each one
            for istep in range(0, nsteps):
                skip_events  = events_to_process * istep
                max_events   = events_to_process #skip_events + events_to_process

                job_id = "_".join( [kwargs["job_id"], data_dsid, str(istep)] )
                #echo "${RUNDIR}run_template.sh ${LABEL}_${id}_${istep} ${INPUT_DIR}group.det-muon.${DATAID}.EXT0._${id}.HITS.pool.root ${skip_events} ${max_events} ${id}"     | qsub -N mu_reco_${DATE}_${LABEL}_${id} -q generic -j oe

                run_cmd  = "{} {} {} {} {}".format(kwargs["fpath_run_script"], job_id, file, skip_events, max_events)
                sub_cmd  = "qsub -N {} -q {} -j oe -o {}".format(job_id, kwargs["queue"], kwargs["fpath_output_log"])
                full_cmd = "".join(["echo ", run_cmd, " | ", sub_cmd])

                #logging.info('Creating job with the following command: {}'.format(full_cmd))
                #Write the command to the file
                f.write(full_cmd + "\n")


    f.close()

# Step 2
def create_submit_script_for_mtester(**kwargs):
    """ Compared to the athena run this does not take any run template but just runs directly package from the MuonTester by calling: python -m MuonTester.runCAFTester """
    logging.info('Creating submit script for the MuonTester with the following arguments: {}'.format(kwargs))

    #find input files
    logging.info('Retrieving input files from the following dir {}'.format(kwargs["input_esd_dir"]))

    #Write a function that uses glob and returns a list of files with the .pool.root extension
    all_esd_files = glob.glob(kwargs["input_esd_dir"] + "/*ESD*.root")

    logging.info('Creating submit script with the following name: {}'.format(kwargs["submit_script_name"]))
    with open(kwargs["submit_script_name"], "w") as f:

       for file in all_esd_files:

        #ATM no splitting into chunks this has been already done in athena run

        esd_file_name = file.split("/")[-1]

        #Removing ESD prefix
        file_id = esd_file_name.replace("OUT_ESD_", "")

        #Removing .root suffix
        file_id = file_id.replace(".root", "")

        #At this point file should contain only job id and tag with istep number e.g. sub_test_1_000040_2
        #Create mt output name
        mt_output = "{}/mt_{}.root".format(kwargs["mt_output_dir"], file_id)

        #Take actually job_id out of the name of the ESD file and append mt prefix
        #job_id = "_".join( [kwargs["job_id"], file_id] )
        job_id = "_".join( ["mt", file_id] )

        run_cmd = "python -m MuonTester.runCAFTester --inputFile {} --outputFile {}".format(file, mt_output)
        #sub_cmd = bsub -J mt_sub_${LABEL}_${TAG} python -m  MuonTester.runCAFTester  --inputFile ${INDIR}/OUT_ESD_${LABEL}_${TAG}.root  --outputFile ${OUT_DIR}mt_${LABEL}_${TAG}.r    oot
        sub_cmd  = "qsub -N {} -q {} -V -j oe -o {}".format(job_id, kwargs["queue"], kwargs["fpath_output_log"])
        full_cmd = "".join(["echo ", run_cmd, " | ", sub_cmd])

        #logging.info('Creating job with the following command: {}'.format(full_cmd))
        #Write the command to the file
        f.write(full_cmd + "\n")

    f.close()

# Step 3
def create_submit_script_for_anantuples(**kwargs):
    """Last step of the processing where the input is ntuple from the mt and output root file with histograms """

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a submit shell script for nikhef batch submission system")


    parser.add_argument(
        "--run_type",
        type=str,
        nargs="?",
        help="Atm either athena or mtester",
        default="athena",
    )

    parser.add_argument(
        "--fpath_run_script",
        type=str,
        nargs="?",
        help="full path of the run script to submit on the batch",
        default=None,
    )
    parser.add_argument(
        "--input_hits_dir",
        type=str,
        nargs="?",
        help="full path of the hits directory",
        default="/dcache/atlas/muonreco/mvozak/HITS/PgunDiMu_Pt10to100R3/",
    )
    parser.add_argument(
        "--submit_script_name",
        type=str,
        nargs="?",
        help="name of the submit script",
        default="sub_test0.sh",
    )
    parser.add_argument(
        "--queue",
        type=str,
        nargs="?",
        help="Name of the queue to submit the job to",
        default=False,
    )
    parser.add_argument(
        "--split_into_chunks",
        type=bool,
        nargs="?",
        help="Splits files into chunks of nevents",
        default=False,
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        nargs="?",
        help="Numbe of events per chunk",
        default=5000,
    )
    parser.add_argument(
        "--n_events_per_file",
        type=int,
        nargs="?",
        help="Number of events in each file. This is hardcoded, in principle can be retrieved from the file itself if needed and if it varies a lot in future",
        default=25000,
    )
    parser.add_argument(
        "--job_id",
        type=str,
        nargs="?",
        help="Name of the job ID",
        default="sub_test0",
    )
    parser.add_argument(
        "--fpath_output_log",
        type=str,
        nargs="?",
        help="Full path of the logs",
        default="out_logs",
    )


    #Below are the input arguments for the MuonTester submission
    #TODO: separate into a different argument parser?

    parser.add_argument(
        "--input_esd_dir",
        type=str,
        nargs="?",
        help="full path of the hits directory",
        default="/dcache/atlas/muonreco/mvozak/ESD_LOCAL_OUTPUT/",
    )
    parser.add_argument(
        "--mt_output_dir",
        type=str,
        nargs="?",
        help="full path of the hits directory",
        default="/project/atlas/users/mvozak/MuonReco/run/submit/test_clustering_algs/mtester/",
    )

    args = vars(parser.parse_args())

    #Load config file
    logging.info('Creating dir for log output file')

    #Create figure output folder if it doesn't exist
    if not os.path.exists(args["fpath_output_log"]):
        logging.info('Creating folder for output: {}'.format(args["fpath_output_log"]) )
        os.makedirs(args["fpath_output_log"])

    #For the preprocessing
    if args["run_type"] == "athena":
        create_submit_script_for_athena(**args)
    elif args["run_type"] == "mtester":

        if not os.path.exists(args["mt_output_dir"]):
            logging.info('Creating folder for MuonTester output: {}'.format(args["mt_output_dir"]) )
            os.makedirs(args["mt_output_dir"])

        create_submit_script_for_mtester(**args)
    else:
        print("Run type not supported")

