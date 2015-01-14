#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "mdhim.h"
#include "mdhim_options.h"

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_db *mdb;
	int key;
	int value;
	int num_wthreads = 4;
	int myrank;
	struct mdhim_brm_t *brm;
	struct mdhim_bgetrm_t *bgrm;
        mdhim_options_t *db_opts;
	MPI_Comm comm;

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }

	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

        db_opts = mdhim_options_init();
        mdhim_options_set_db_path(db_opts, "./mdhim-db-dir");
        mdhim_options_set_db_name(db_opts, "mdhim");
        mdhim_options_set_db_type(db_opts, LEVELDB);
        mdhim_options_set_key_type(db_opts, MDHIM_INT_KEY); //Key_type = 1 (int)
	//mdhim_options_set_debug_level(db_opts, MLOG_CRIT);
	mdhim_options_set_debug_level(db_opts, MLOG_INFO);
        mdhim_options_set_server_factor(db_opts, 1);

	comm = MPI_COMM_WORLD;
	ret = mdhimInit(&comm, num_wthreads);
	if (ret != MDHIM_SUCCESS) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}

	//Open database
	mdb = mdhimOpen(db_opts);
	if (mdb == NULL) {
		printf("Error opening database.\n");
		exit(1);
	} else {
		printf("Opening database successfully.\n");
	}

	//Put the keys and values
	key = 100 * (myrank + 1);
	value = 500 * (myrank + 1);
	brm = mdhimPut(mdb, &key, sizeof(key),
		       &value, sizeof(value),
		       NULL, NULL);
	if (!brm || brm->error) {
		printf("Error inserting key/value into MDHIM\n");
		exit(1);
	} else {
		printf("Successfully inserted key/value into MDHIM\n");
	}

	mdhim_full_release_msg(brm);
	//Commit the database
	ret = mdhimCommit(mdb);
	if (ret != MDHIM_SUCCESS) {
		printf("Error committing MDHIM database\n");
		exit(1);
	} else {
		printf("Committed MDHIM database\n");
	}

	//Get the values
	value = 0;
	bgrm = mdhimGet(mdb, mdb->primary_index, &key, sizeof(key), MDHIM_GET_EQ);
	if (!bgrm || bgrm->error) {
		printf("Error getting value for key: %d from MDHIM\n", key);
		exit(1);
	} else {
		printf("Successfully got value: %d from MDHIM\n", *((int *) bgrm->values[0]));
	}

	mdhim_full_release_msg(bgrm);

	MPI_Barrier(MPI_COMM_WORLD);
	ret = mdhimStatFlush(mdb, mdb->primary_index);
	if (ret != MDHIM_SUCCESS) {
		printf("Error statflush database.\n");
		exit(1);
	} else {
		printf("Database statflush successfully.\n");
	}

	ret = mdhimClose(mdb);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing database.\n");
		exit(1);
	} else {
		printf("Closing database successfully.\n");
	}

	mdhim_options_destroy(db_opts);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

	MPI_Barrier(MPI_COMM_WORLD);

	ret = mdhimFinalize();
	if (ret != MDHIM_SUCCESS) {
		printf("Error finalizing MDHIM.\n");
		exit(1);
	} else {
		printf("Finalizing successfully.\n");
	}

	return 0;
}
