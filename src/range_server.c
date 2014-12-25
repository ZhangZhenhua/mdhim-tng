/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <linux/limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mdhim.h"
#include "range_server.h"
#include "partitioner.h"
#include "mdhim_options.h"

void add_timing(struct timeval start, struct timeval end, int num, 
		struct mdhim_t *md, int mtype) {
	long double elapsed;

	elapsed = (long double) (end.tv_sec - start.tv_sec) + 
		((long double) (end.tv_usec - start.tv_usec)/1000000.0);
	if (mtype == MDHIM_PUT || mtype == MDHIM_BULK_PUT) {
		md->mdhim_rs->put_time += elapsed;
		md->mdhim_rs->num_put += num;
	} else if (mtype == MDHIM_BULK_GET) {
		md->mdhim_rs->get_time += elapsed;
		md->mdhim_rs->num_get += num;
	}
}

/**
 * send_locally_or_remote
 * Sends the message remotely or locally
 *
 * @param md       Pointer to the main MDHIM structure
 * @param dest     Destination rank
 * @param message  pointer to message to send
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_locally_or_remote(struct mdhim_t *md, int dest, void *message) {
	int ret = MDHIM_SUCCESS;
	MPI_Request **size_req, **msg_req;
	int *sizebuf;
	void **sendbuf;

	if (md->mdhim_rank != dest) {
		//Sends the message remotely
		size_req = malloc(sizeof(MPI_Request *));
		msg_req = malloc(sizeof(MPI_Request *));
		sendbuf = malloc(sizeof(void *));
		sizebuf = malloc(sizeof(int));
		ret = send_client_response(md, dest, message, sizebuf, 
					   sendbuf, size_req, msg_req);
		if (*size_req) {
			range_server_add_oreq(md, *size_req, sizebuf);
		} else {
		  free(sizebuf);
		}

		if (*msg_req) {
			range_server_add_oreq(md, *msg_req, *sendbuf);
		} else if (*sendbuf) {
			free(*sendbuf);
		}
		
		free(sendbuf);
		mdhim_full_release_msg(message);
		free(size_req);
		free(msg_req);
	} else {
		//Sends the message locally
		pthread_mutex_lock(md->receive_msg_mutex);
		md->receive_msg = message;
		pthread_mutex_unlock(md->receive_msg_mutex);
		pthread_cond_signal(md->receive_msg_ready_cv);
	}

	return ret;
}

struct index_t *find_index(struct mdhim_t *md, struct mdhim_basem_t *msg) {
	struct index_t *ret;
       
	ret = get_index(md, msg->index);

	return ret;

}

/**
 * update_stat
 * Adds or updates the given stat to the hash table
 *
 * @param key      pointer to the key we are examining
 * @param key_len  the key's length
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int update_stat(mdhim_open_db_t *odb, void *key, uint32_t key_len) {
	int slice_num;
	void *val1, *val2;
	int float_type = 0;
	struct mdhim_stat *os, *stat;
	struct mdhim_store_t *mdhim_store;

	mdhim_store = odb->mdhim_store;
	//Acquire the lock to update the stats
	while (pthread_rwlock_wrlock(mdhim_store->mdhim_store_stats_lock) == EBUSY) {
		usleep(10);
	}

	if ((float_type = is_float_key(odb->db_key_type)) == 1) {
		val1 = (void *) malloc(sizeof(long double));
		val2 = (void *) malloc(sizeof(long double));
	} else {
		val1 = (void *) malloc(sizeof(uint64_t));
		val2 = (void *) malloc(sizeof(uint64_t));
	}

	if (odb->db_key_type == MDHIM_STRING_KEY) {
		*(long double *)val1 = get_str_num(key, key_len);
		*(long double *)val2 = *(long double *)val1;
	} else if (odb->db_key_type == MDHIM_FLOAT_KEY) {
		*(long double *)val1 = *(float *) key;
		*(long double *)val2 = *(float *) key;
	} else if (odb->db_key_type == MDHIM_DOUBLE_KEY) {
		*(long double *)val1 = *(double *) key;
		*(long double *)val2 = *(double *) key;
	} else if (odb->db_key_type == MDHIM_INT_KEY) {
		*(uint64_t *)val1 = *(uint32_t *) key;
		*(uint64_t *)val2 = *(uint32_t *) key;
	} else if (odb->db_key_type == MDHIM_LONG_INT_KEY) {
		*(uint64_t *)val1 = *(uint64_t *) key;
		*(uint64_t *)val2 = *(uint64_t *) key;
	} else if (odb->db_key_type == MDHIM_BYTE_KEY) {
		*(long double *)val1 = get_byte_num(key, key_len);
		*(long double *)val2 = *(long double *)val1;
	}

	slice_num = get_slice_num(odb->max_recs_per_slice, odb->db_key_type,
				  key, key_len);

	HASH_FIND_INT(mdhim_store->mdhim_store_stats, &slice_num, os);

	stat = malloc(sizeof(struct mdhim_stat));
	stat->min = val1;
	stat->max = val2;
	stat->num = 1;
	stat->key = slice_num;
	stat->dirty = 1;

	if (float_type && os) {
		if (*(long double *)os->min > *(long double *)val1) {
			free(os->min);
			stat->min = val1;
		} else {
			free(val1);
			stat->min = os->min;
		}

		if (*(long double *)os->max < *(long double *)val2) {
			free(os->max);
			stat->max = val2;
		} else {
			free(val2);
			stat->max = os->max;
		}
	}
	if (!float_type && os) {
		if (*(uint64_t *)os->min > *(uint64_t *)val1) {
			free(os->min);
			stat->min = val1;
		} else {
			free(val1);
			stat->min = os->min;
		}

		if (*(uint64_t *)os->max < *(uint64_t *)val2) {
			free(os->max);
			stat->max = val2;
		} else {
			free(val2);
			stat->max = os->max;
		}
	}

	if (!os) {
		HASH_ADD_INT(mdhim_store->mdhim_store_stats, key, stat);
	} else {
		stat->num = os->num + 1;
		//Replace the existing stat
		HASH_REPLACE_INT(mdhim_store->mdhim_store_stats, key, stat, os);
		free(os);
	}

	//Release the stats lock
	pthread_rwlock_unlock(mdhim_store->mdhim_store_stats_lock);
	return MDHIM_SUCCESS;
}

/**
 * load_stats
 * Loads the statistics from the database
 *
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int load_stats(mdhim_open_db_t *odb) {
	void **val;
	int *val_len, *key_len;
	int **slice;
	int *old_slice;
	struct mdhim_stat *stat;
	int float_type = 0;
	void *min, *max;
	int done = 0;

	float_type = is_float_key(odb->db_key_type);
	slice = malloc(sizeof(int *));
	*slice = NULL;
	key_len = malloc(sizeof(int));
	*key_len = sizeof(int);
	val = malloc(sizeof(struct mdhim_db_stat *));
	val_len = malloc(sizeof(int));
	old_slice = NULL;
	odb->mdhim_store->mdhim_store_stats = NULL;
	while (!done) {
		//Check the db for the key/value
		*val = NULL;
		*val_len = 0;
		odb->mdhim_store->get_next(odb->mdhim_store->db_stats,
					     (void **) slice, key_len, (void **) val, 
					     val_len);

		//Add the stat to the hash table - the value is 0 if the key was not in the db
		if (!*val || !*val_len) {
			done = 1;
			continue;
		}

		if (old_slice) {
			free(old_slice);
			old_slice = NULL;
		}

		mlog(MDHIM_SERVER_DBG, "Rank: %d - Loaded stat for slice: %d with " 
		     "imin: %lu and imax: %lu, dmin: %Lf, dmax: %Lf, and num: %lu", 
		     mdhim_gdata.mdhim_rank, **slice, (*(struct mdhim_db_stat **)val)->imin, 
		     (*(struct mdhim_db_stat **)val)->imax, (*(struct mdhim_db_stat **)val)->dmin, 
		     (*(struct mdhim_db_stat **)val)->dmax, (*(struct mdhim_db_stat **)val)->num);

		stat = malloc(sizeof(struct mdhim_stat));
		if (float_type) {
			min = (void *) malloc(sizeof(long double));
			max = (void *) malloc(sizeof(long double));
			*(long double *)min = (*(struct mdhim_db_stat **)val)->dmin;
			*(long double *)max = (*(struct mdhim_db_stat **)val)->dmax;
		} else {
			min = (void *) malloc(sizeof(uint64_t));
			max = (void *) malloc(sizeof(uint64_t));
			*(uint64_t *)min = (*(struct mdhim_db_stat **)val)->imin;
			*(uint64_t *)max = (*(struct mdhim_db_stat **)val)->imax;
		}

		stat->min = min;
		stat->max = max;
		stat->num = (*(struct mdhim_db_stat **)val)->num;
		stat->key = **slice;
		stat->dirty = 0;
		old_slice = *slice;
		HASH_ADD_INT(odb->mdhim_store->mdhim_store_stats, key, stat);
		free(*val);
	}

	if (old_slice) {
		free(old_slice);
	}
	free(val);
	free(val_len);
	free(key_len);
	free(*slice);
	free(slice);
	return MDHIM_SUCCESS;
}

/**
 * write_stats
 * Writes the statistics stored in a hash table to the database
 * This is done on mdhim_close
 *
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int write_stats(mdhim_open_db_t *odb) {
	struct mdhim_stat *stat, *tmp;
	struct mdhim_db_stat *dbstat;
	int float_type = 0;

	float_type = is_float_key(odb->db_key_type);

	//Iterate through the stat hash entries
	HASH_ITER(hh, odb->mdhim_store->mdhim_store_stats, stat, tmp) {
		if (!stat) {
			continue;
		}

		if (!stat->dirty) {
			goto free_stat;
		}

		dbstat = malloc(sizeof(struct mdhim_db_stat));
		if (float_type) {
			dbstat->dmax = *(long double *)stat->max;
			dbstat->dmin = *(long double *)stat->min;
			dbstat->imax = 0;
			dbstat->imin = 0;
		} else {
			dbstat->imax = *(uint64_t *)stat->max;
			dbstat->imin = *(uint64_t *)stat->min;
			dbstat->dmax = 0;
			dbstat->dmin = 0;
		}

		dbstat->slice = stat->key;
		dbstat->num = stat->num;
		//Write the key to the database
		odb->mdhim_store->put(odb->mdhim_store->db_stats,
				     &dbstat->slice, sizeof(int), dbstat,
				     sizeof(struct mdhim_db_stat));
		//Delete and free hash entry
		free(dbstat);

	free_stat:
		HASH_DEL(odb->mdhim_store->mdhim_store_stats, stat);
		free(stat->max);
		free(stat->min);
		free(stat);
	}

	return MDHIM_SUCCESS;
}

/**
 * range_server_add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param md      Pointer to the main MDHIM structure
 * @param item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS
 */
int range_server_add_work(struct mdhim_t *md, work_item *item) {
	//Lock the work queue mutex
	pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
	item->next = NULL;
	item->prev = NULL;       
	
	//Add work to the tail of the work queue
	if (md->mdhim_rs->work_queue->tail) {
		md->mdhim_rs->work_queue->tail->next = item;
		item->prev = md->mdhim_rs->work_queue->tail;
		md->mdhim_rs->work_queue->tail = item;
	} else {
		md->mdhim_rs->work_queue->head = item;
		md->mdhim_rs->work_queue->tail = item;
	}

	//Signal the waiting thread that there is work available
	pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);
	pthread_cond_signal(md->mdhim_rs->work_ready_cv);

	return MDHIM_SUCCESS;
}

/**
 * get_work
 * Returns the next work item from the work queue
 *
 * @param md  Pointer to the main MDHIM structure
 * @return  the next work_item to process
 */

work_item *get_work(struct mdhim_t *md) {
	work_item *item;

	item = md->mdhim_rs->work_queue->head;
	if (!item) {
		return NULL;
	}

	//Set the list head and tail to NULL
	md->mdhim_rs->work_queue->head = NULL;
	md->mdhim_rs->work_queue->tail = NULL;

	//Return the list
	return item;
}

/* XXX add lock to protect this hash table for the open/close
 * race. Well behavied applications should not have such issues. */
int _add_opendb(mdhim_open_db_t *opendb) {
	mdhim_open_db_t *opendbs = mdhim_gdata.mdhim_rs->opendbs;

	HASH_ADD_STR(opendbs, db_path, opendb);
	return 0;
}

int _del_opendb(char *path) {
	mdhim_open_db_t *outdb = NULL;
	mdhim_open_db_t *opendbs = mdhim_gdata.mdhim_rs->opendbs;

	HASH_FIND_STR(opendbs, path, outdb);
	if (outdb != NULL) {
		HASH_DEL(opendbs, outdb);
		free(outdb);
	}
	return 0;
}

mdhim_open_db_t* _find_opendb(char *path) {
	mdhim_open_db_t *outdb = NULL;
	mdhim_open_db_t *opendbs = mdhim_gdata.mdhim_rs->opendbs;

	HASH_FIND_STR(opendbs, path, outdb);

	return outdb;
}

/* increase refcount if finding opendb in hash */
mdhim_open_db_t* _find_opendb_inc_ref(char *path) {
	mdhim_open_db_t *outdb = NULL;
	mdhim_open_db_t *opendbs = mdhim_gdata.mdhim_rs->opendbs;

	HASH_FIND_STR(opendbs, path, outdb);
	if (outdb != NULL) outdb->ref ++;

	return outdb;
}

/* decrease refcount if finding opendb in hash */
mdhim_open_db_t* _find_opendb_dec_ref(char *path) {
	mdhim_open_db_t *outdb = NULL;
	mdhim_open_db_t *opendbs = mdhim_gdata.mdhim_rs->opendbs;

	HASH_FIND_STR(opendbs, path, outdb);
	if (outdb != NULL && outdb->ref > 0) {
		outdb->ref --;
		if (outdb->ref == 0) {
			/* Just remove it from hash table. Caller will
			 * take care of this zero-refcount object.
			 */
			HASH_DEL(opendbs, outdb);
		}
	}

	return outdb;
}

/**
 * range_server_stop
 * Stop the range server (i.e., stops the threads and frees the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_stop(struct mdhim_t *md) {
	int i, ret;
	work_item *head, *temp_item;

	//Signal to the listener thread that it needs to shutdown
	md->shutdown = 1;

	/* Wait for the threads to finish */
	pthread_cond_broadcast(md->mdhim_rs->work_ready_cv);
	pthread_join(md->mdhim_rs->listener, NULL);
	/* Wait for the threads to finish */
	for (i = 0; i < md->mdhim_rs->num_wthreads; i++) {
		pthread_join(*md->mdhim_rs->workers[i], NULL);
		free(md->mdhim_rs->workers[i]);
	}
	free(md->mdhim_rs->workers);
		
	//Destroy the condition variables
	if ((ret = pthread_cond_destroy(md->mdhim_rs->work_ready_cv)) != 0) {
	  mlog(MDHIM_SERVER_DBG, "Rank: %d - Error destroying work cond variable", 
	       md->mdhim_rank);
	}
	free(md->mdhim_rs->work_ready_cv);
		
	//Destroy the work queue mutex
	if ((ret = pthread_mutex_destroy(md->mdhim_rs->work_queue_mutex)) != 0) {
	  mlog(MDHIM_SERVER_DBG, "Rank: %d - Error destroying work queue mutex", 
	       md->mdhim_rank);
	}
	free(md->mdhim_rs->work_queue_mutex);
		
	//Clean outstanding sends
	range_server_clean_oreqs(md);
	//Destroy the out req mutex
	if ((ret = pthread_mutex_destroy(md->mdhim_rs->out_req_mutex)) != 0) {
	  mlog(MDHIM_SERVER_DBG, "Rank: %d - Error destroying work queue mutex", 
	       md->mdhim_rank);
	}
	free(md->mdhim_rs->out_req_mutex);
		
	//Free the work queue
	head = md->mdhim_rs->work_queue->head;
	while (head) {
	  temp_item = head->next;
	  free(head);
	  head = temp_item;
	}
	free(md->mdhim_rs->work_queue);
		
	mlog(MDHIM_SERVER_INFO, "Rank: %d - Inserted: %ld records in %Lf seconds", 
	     md->mdhim_rank, md->mdhim_rs->num_put, md->mdhim_rs->put_time);
	mlog(MDHIM_SERVER_INFO, "Rank: %d - Retrieved: %ld records in %Lf seconds", 
	     md->mdhim_rank, md->mdhim_rs->num_get, md->mdhim_rs->get_time);
	  
	//Free the range server data
	free(md->mdhim_rs);
	md->mdhim_rs = NULL;

	return MDHIM_SUCCESS;
}

/**
 * range_server_open
 * Handles the open message and create/open database
 *
 * @param md        pointer to the main MDHIM struct
 * @param om        pointer to the open message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_open(struct mdhim_t *md, struct mdhim_openm_t *om, int source) {
	int ret = MDHIM_SUCCESS, flags = MDHIM_CREATE;
	struct mdhim_rm_t *rm;
	struct mdhim_store_t *mdhim_store = NULL;
	mdhim_open_db_t *opendb = NULL;

	opendb = _find_opendb_inc_ref(om->basem.db_path);
	if (opendb != NULL) {
		goto out;
	}

	opendb = malloc(sizeof(mdhim_open_db_t));
	if (opendb == NULL) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d: Error while allocating "
		     "memory.", mdhim_gdata.mdhim_rank);
		ret = MDHIM_ERROR;
		goto out;
	}

	/* initialize mdhim_store */
	mdhim_store = mdhim_db_init(om->db_type);

	ret = mdhim_store->open(&mdhim_store->db_handle, &mdhim_store->db_stats,
				om->basem.db_path, flags, om->db_key_type,
				NULL /* letting opts = NULL*/);
	if (ret != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d: Error while opening "
		     "database, ret %d.", mdhim_gdata.mdhim_rank, ret);
		free(opendb);
		ret = MDHIM_ERROR;
		goto out;
	}

	/* initialize and add mdhim_store in opendbs table */
	opendb->ref = 1;
	opendb->db_type = om->db_type;
	opendb->db_key_type = om->db_key_type;
	opendb->db_value_append = om->db_value_append;
	opendb->debug_level = om->debug_level;
	opendb->max_recs_per_slice = om->max_recs_per_slice;
	strcpy(opendb->db_path, om->basem.db_path);
	opendb->mdhim_store = mdhim_store;

	_add_opendb(opendb);

out:
	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->basem.server_rank = mdhim_gdata.mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, rm);
	return ret;
}

/**
 * range_server_close
 * Handles the close message and close database
 *
 * @param md        pointer to the main MDHIM struct
 * @param cm        pointer to the open message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_close(struct mdhim_t *md, struct mdhim_closem_t *cm, int source) {
	int ret = MDHIM_SUCCESS;
	struct mdhim_rm_t *rm;
	struct mdhim_store_t *mdhim_store = NULL;
	mdhim_open_db_t *opendb = NULL;

	opendb = _find_opendb_dec_ref(cm->basem.db_path);
	if (opendb == NULL) {
		/* return error??? */
		goto out;
	}

	if (opendb->ref > 0) {
		/* other siblings hold its ref */
		goto out;
	}

	/* close it for real */
	mdhim_store = opendb->mdhim_store;
	ret = mdhim_store->close(mdhim_store->db_handle, mdhim_store->db_stats);
	if (ret != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d: Error while closeing "
		     "database, ret %d.", mdhim_gdata.mdhim_rank, ret);
		ret = MDHIM_ERROR;
		goto out;
	}
	free(opendb);

out:
	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->basem.server_rank = mdhim_gdata.mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, rm);
	return ret;
}

/**
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the put message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_put(struct mdhim_t *md, struct mdhim_putm_t *im, int source) {
	int ret;
	struct mdhim_rm_t *rm;
	int error = 0;
	void **value;
	int32_t *value_len;
	int exists = 0;
	void *new_value;
	int32_t new_value_len;
	void *old_value;
	int32_t old_value_len;
	struct timeval start, end;
	int inserted = 0;
	int db_value_append;
	//struct index_t *index;
	mdhim_open_db_t *opendb = NULL;

	value = malloc(sizeof(void *));
	*value = NULL;
	value_len = malloc(sizeof(int32_t));
	*value_len = 0;

	//Get the index referenced the message
	opendb = _find_opendb_inc_ref(im->basem.db_path);
	if (!opendb) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving open db: %s", 
		     mdhim_gdata.mdhim_rank, im->basem.db_path);
		error = MDHIM_ERROR;
		goto done;
	}
	db_value_append = opendb->db_value_append;

	gettimeofday(&start, NULL);
       //Check for the key's existence
/*	index->mdhim_store->get(index->mdhim_store->db_handle, 
				       im->key, im->key_len, value, 
				       value_len);
*/
	//The key already exists
	if (*value && *value_len) {
		exists = 1;
	}

        //If the option to append was specified and there is old data, concat the old and new
	if (exists && db_value_append == MDHIM_DB_APPEND) {
		old_value = *value;
		old_value_len = *value_len;
		new_value_len = old_value_len + im->value_len;
		new_value = malloc(new_value_len);
		memcpy(new_value, old_value, old_value_len);
		memcpy(new_value + old_value_len, im->value, im->value_len);
	} else {
		new_value = im->value;
		new_value_len = im->value_len;
	}
    
	if (*value && *value_len) {
		free(*value);
	}
	free(value);
	free(value_len);
        //Put the record in the database
	if ((ret =
	     opendb->mdhim_store->put(opendb->mdhim_store->db_handle,
				     im->key, im->key_len, new_value,
				     new_value_len)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error putting record",
		     mdhim_gdata.mdhim_rank);
		error = ret;
	} else {
		inserted = 1;
	}

	if (!exists && error == MDHIM_SUCCESS) {
		update_stat(opendb, im->key, im->key_len);
	}
	_find_opendb_dec_ref(im->basem.db_path);

	gettimeofday(&end, NULL);
	add_timing(start, end, inserted, &mdhim_gdata, MDHIM_PUT);

done:
	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = error;
	//Set the server's rank
	rm->basem.server_rank = md->mdhim_rank;
	
	//Send response
	ret = send_locally_or_remote(&mdhim_gdata, source, rm);

	//Free memory
	if (exists && db_value_append == MDHIM_DB_APPEND) {
		free(new_value);
	}
	if (source != mdhim_gdata.mdhim_rank) {
		free(im->key);
		free(im->value);
	} 
	free(im);
	
	return MDHIM_SUCCESS;
}


/**
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bim       pointer to the bulk put message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bput(struct mdhim_t *md, struct mdhim_bputm_t *bim, int source) {
	int i;
	int ret;
	int error = MDHIM_SUCCESS;
	struct mdhim_rm_t *brm;
	void **value;
	int32_t *value_len;
	int *exists;
	void *new_value;
	int32_t new_value_len;
	void **new_values;
	int32_t *new_value_lens;
	void *old_value;
	int32_t old_value_len;
	struct timeval start, end;
	int num_put = 0;
	int db_value_append;
	//struct index_t *index;
	mdhim_open_db_t *opendb = NULL;

	gettimeofday(&start, NULL);
	exists = malloc(bim->num_keys * sizeof(int));
	new_values = malloc(bim->num_keys * sizeof(void *));
	new_value_lens = malloc(bim->num_keys * sizeof(int));
	value = malloc(sizeof(void *));
	value_len = malloc(sizeof(int32_t));

	//Get the index referenced the message
	opendb = _find_opendb_inc_ref(bim->basem.db_path);
	if (!opendb) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving opendb for: %s", 
		     md->mdhim_rank, bim->basem.db_path);
		error = MDHIM_ERROR;
		goto done;
	}
	db_value_append = opendb->db_value_append;

	//Iterate through the arrays and insert each record
	for (i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {	
		*value = NULL;
		*value_len = 0;

                //Check for the key's existence
		opendb->mdhim_store->get(opendb->mdhim_store->db_handle, 
					       bim->keys[i], bim->key_lens[i], value, 
					       value_len);
		//The key already exists
		if (*value && *value_len) {
			exists[i] = 1;
		} else {
			exists[i] = 0;
		}

		//If the option to append was specified and there is old data, concat the old and new
		if (exists[i] && db_value_append == MDHIM_DB_APPEND) {
			old_value = *value;
			old_value_len = *value_len;
			new_value_len = old_value_len + bim->value_lens[i];
			new_value = malloc(new_value_len);
			memcpy(new_value, old_value, old_value_len);
			memcpy(new_value + old_value_len, bim->values[i], bim->value_lens[i]);		
			if (exists[i] && source != md->mdhim_rank) {
				free(bim->values[i]);
			}

			new_values[i] = new_value;
			new_value_lens[i] = new_value_len;
		} else {
			new_values[i] = bim->values[i];
			new_value_lens[i] = bim->value_lens[i];
		}
		
		if (*value) {
			free(*value);
		}	
	}

	//Put the record in the database
	if ((ret = 
	     opendb->mdhim_store->batch_put(opendb->mdhim_store->db_handle, 
					   bim->keys, bim->key_lens, new_values, 
					   new_value_lens, bim->num_keys)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error batch putting records", 
		     md->mdhim_rank);
		error = ret;
	} else {
		num_put = bim->num_keys;
	}

	for (i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {
		//Update the stats if this key didn't exist before
		if (!exists[i] && error == MDHIM_SUCCESS) {
			update_stat(opendb, bim->keys[i], bim->key_lens[i]);
		}
	       
		if (exists[i] && db_value_append == MDHIM_DB_APPEND) {
			//Release the value created for appending the new and old value
			free(new_values[i]);
		}		

		//Release the bput keys/value if the message isn't coming from myself
		if (source != md->mdhim_rank) {
			free(bim->keys[i]);
			free(bim->values[i]);
		} 
	}

	free(exists);
	free(new_values);
	free(new_value_lens);
	free(value);
	free(value_len);
	gettimeofday(&end, NULL);
	add_timing(start, end, num_put, md, MDHIM_BULK_PUT);

	_find_opendb_dec_ref(bim->basem.db_path);
 done:
	//Create the response message
	brm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	brm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	brm->error = error;
	//Set the server's rank
	brm->basem.server_rank = md->mdhim_rank;

	//Release the internals of the bput message
	free(bim->keys);
	free(bim->key_lens);
	free(bim->values);
	free(bim->value_lens);
	free(bim);

	//Send response
	ret = send_locally_or_remote(md, source, brm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_del
 * Handles the delete message and deletes the data from the database
 *
 * @param md       Pointer to the main MDHIM struct
 * @param dm       pointer to the delete message to handle
 * @param source   source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_del(struct mdhim_t *md, struct mdhim_delm_t *dm, int source) {
	int ret = MDHIM_ERROR;
	struct mdhim_rm_t *rm;
	struct index_t *index;

	//Get the index referenced the message
	index = find_index(md, (struct mdhim_basem_t *) dm);
	if (!index) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving index for id: %d", 
		     md->mdhim_rank, dm->basem.index);
		ret = MDHIM_ERROR;
		goto done;
	}

	//Put the record in the database
	if ((ret = 
	     index->mdhim_store->del(index->mdhim_store->db_handle, 
				     dm->key, dm->key_len)) != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error deleting record", 
		     md->mdhim_rank);
	}

 done:
	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->basem.server_rank = md->mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, rm);
	free(dm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bdel
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bdm       pointer to the bulk delete message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(struct mdhim_t *md, struct mdhim_bdelm_t *bdm, int source) {
 	int i;
	int ret;
	int error = 0;
	struct mdhim_rm_t *brm;
	struct index_t *index;

	//Get the index referenced the message
	index = find_index(md, (struct mdhim_basem_t *) bdm);
	if (!index) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving index for id: %d", 
		     md->mdhim_rank, bdm->basem.index);
		error = MDHIM_ERROR;
		goto done;
	}

	//Iterate through the arrays and delete each record
	for (i = 0; i < bdm->num_keys && i < MAX_BULK_OPS; i++) {
		//Put the record in the database
		if ((ret = 
		     index->mdhim_store->del(index->mdhim_store->db_handle, 
					     bdm->keys[i], bdm->key_lens[i])) 
		    != MDHIM_SUCCESS) {
			mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error deleting record", 
			     md->mdhim_rank);
			error = ret;
		}
	}

done:
	//Create the response message
	brm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	brm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	brm->error = error;
	//Set the server's rank
	brm->basem.server_rank = md->mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, brm);
	free(bdm->keys);
	free(bdm->key_lens);
	free(bdm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_commit
 * Handles the commit message and commits outstanding writes to the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the commit message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_commit(struct mdhim_t *md, struct mdhim_basem_t *im, int source) {
	int ret;
	struct mdhim_rm_t *rm;
	struct index_t *index;

	//Get the index referenced the message
	index = find_index(md, (struct mdhim_basem_t *) im);
	if (!index) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving index for id: %d", 
		     md->mdhim_rank, im->index);
		ret = MDHIM_ERROR;
		goto done;
	}

        //Put the record in the database
	if ((ret = 
	     index->mdhim_store->commit(index->mdhim_store->db_handle)) 
	    != MDHIM_SUCCESS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error committing database", 
		     md->mdhim_rank);	
	}

 done:	
	//Create the response message
	rm = malloc(sizeof(struct mdhim_rm_t));
	//Set the type
	rm->basem.mtype = MDHIM_RECV;
	//Set the operation return code as the error
	rm->error = ret;
	//Set the server's rank
	rm->basem.server_rank = md->mdhim_rank;

	//Send response
	ret = send_locally_or_remote(md, source, rm);
	free(im);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bget
 * Handles the bulk get message, retrieves the data from the database, and sends the results back
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param bgm       pointer to the bulk get message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, int source) {
	int ret;
	void **values;
	int32_t *value_lens;
	int i;
	struct mdhim_bgetrm_t *bgrm;
	int error = 0;
	struct timeval start, end;
	int num_retrieved = 0;
	mdhim_open_db_t *opendb = NULL;
	//struct index_t *index;

	gettimeofday(&start, NULL);
	values = malloc(sizeof(void *) * bgm->num_keys);
	value_lens = malloc(sizeof(int32_t) * bgm->num_keys);
	memset(value_lens, 0, sizeof(int32_t) * bgm->num_keys);

	//Get the index referenced the message
	opendb = _find_opendb_inc_ref(bgm->basem.db_path);
	if (!opendb) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving opendb for : %s", 
		     md->mdhim_rank, bgm->basem.db_path);
		error = MDHIM_ERROR;
		goto done;
	}

	//Iterate through the arrays and get each record
	for (i = 0; i < bgm->num_keys && i < MAX_BULK_OPS; i++) {
		switch(bgm->op) {
			// Gets the value for the given key
		case MDHIM_GET_EQ:
			//Get records from the database
			if ((ret =
			     opendb->mdhim_store->get(opendb->mdhim_store->db_handle, 
						     bgm->keys[i], bgm->key_lens[i], &values[i], 
						     &value_lens[i])) != MDHIM_SUCCESS) {
				error = ret;
				value_lens[i] = 0;
				values[i] = NULL;
				continue;
			}

			break;
			/* Gets the next key and value that is in order after the passed in key */
		case MDHIM_GET_NEXT:
			if ((ret =
			     opendb->mdhim_store->get_next(opendb->mdhim_store->db_handle, 
							  &bgm->keys[i], &bgm->key_lens[i], &values[i], 
							  &value_lens[i])) != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Error getting record",
						mdhim_gdata.mdhim_rank);
				error = ret;
				value_lens[i] = 0;
				values[i] = NULL;
				continue;
			}
			
			break;
			/* Gets the previous key and value that is in order before the passed in key
			   or the last key if no key was passed in */
		case MDHIM_GET_PREV:
			if ((ret =
			     opendb->mdhim_store->get_prev(opendb->mdhim_store->db_handle, 
							  &bgm->keys[i], &bgm->key_lens[i], &values[i], 
							  &value_lens[i])) != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Error getting record",
						mdhim_gdata.mdhim_rank);
				error = ret;
				value_lens[i] = 0;
				values[i] = NULL;
				continue;
			}

			break;
			/* Gets the first key/value */
		case MDHIM_GET_FIRST:
			if ((ret =
			     opendb->mdhim_store->get_next(opendb->mdhim_store->db_handle, 
							  &bgm->keys[i], 0, &values[i], 
							  &value_lens[i])) != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Error getting record",
						mdhim_gdata.mdhim_rank);
				error = ret;
				value_lens[i] = 0;
				values[i] = NULL;
				continue;
			}

			break;
			/* Gets the last key/value */
		case MDHIM_GET_LAST:
			if ((ret =
			     opendb->mdhim_store->get_prev(opendb->mdhim_store->db_handle, 
							  &bgm->keys[i], 0, &values[i], 
							  &value_lens[i])) != MDHIM_SUCCESS) {
				mlog(MDHIM_SERVER_DBG, "Rank: %d - Error getting record",
						mdhim_gdata.mdhim_rank);
				error = ret;
				value_lens[i] = 0;
				values[i] = NULL;
				continue;
			}

			break;
		default:
			mlog(MDHIM_SERVER_DBG, "Rank: %d - Invalid operation: %d "
					"given in range_server_get",
					mdhim_gdata.mdhim_rank, bgm->op);
			continue;
		}

		num_retrieved++;
	}
	_find_opendb_dec_ref(bgm->basem.db_path);

	gettimeofday(&end, NULL);
	add_timing(start, end, num_retrieved, md, MDHIM_BULK_GET);

done:
	//Create the response message
	bgrm = malloc(sizeof(struct mdhim_bgetrm_t));
	//Set the type
	bgrm->basem.mtype = MDHIM_RECV_BULK_GET;
	//Set the operation return code as the error
	bgrm->error = error;
	//Set the server's rank
	bgrm->basem.server_rank = mdhim_gdata.mdhim_rank;
	//Set the key and value
	if (source == mdhim_gdata.mdhim_rank) {
		//If this message is coming from myself, copy the keys
		bgrm->key_lens = malloc(bgm->num_keys * sizeof(int));
		bgrm->keys = malloc(bgm->num_keys * sizeof(void *));
		for (i = 0; i < bgm->num_keys; i++) {
			bgrm->key_lens[i] = bgm->key_lens[i];
			bgrm->keys[i] = malloc(bgrm->key_lens[i]);
			memcpy(bgrm->keys[i], bgm->keys[i], bgrm->key_lens[i]);
		}

		free(bgm->keys);
		free(bgm->key_lens);
	} else {
		bgrm->keys = bgm->keys;
		bgrm->key_lens = bgm->key_lens;
	}

	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_keys = bgm->num_keys;
	//bgrm->basem.index = index->id;
	//bgrm->basem.index_type = index->type;

	//Send response
	ret = send_locally_or_remote(&mdhim_gdata, source, bgrm);

	//Release the bget message
	free(bgm);

	return MDHIM_SUCCESS;
}

/**
 * range_server_bget_op
 * Handles the get message given an op and number of records greater than 1
 * 
 * @param md        Pointer to the main MDHIM struct
 * @param gm        pointer to the get message to handle
 * @param source    source of the message
 * @param op        operation to perform
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bget_op(struct mdhim_t *md, struct mdhim_bgetm_t *bgm, int source, int op) {
	int error = 0;
	void **values;
	void **keys;
	void **get_key; //Used for passing the key to the db
	int *get_key_len; //Used for passing the key len to the db
	void **get_value;
	int *get_value_len;
	int32_t *key_lens;
	int32_t *value_lens;
	struct mdhim_bgetrm_t *bgrm;
	int ret;
	int i, j;
	int num_records;
	int inc_ref = 0;
	struct timeval start, end;
	mdhim_open_db_t *opendb = NULL;
	//struct index_t *index;

	//Initialize pointers and lengths
	values = malloc(sizeof(void *) * bgm->num_keys * bgm->num_recs);
	value_lens = malloc(sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
	memset(value_lens, 0, sizeof(int32_t) *bgm->num_keys * bgm->num_recs);
	keys = malloc(sizeof(void *) * bgm->num_keys * bgm->num_recs);
	memset(keys, 0, sizeof(void *) * bgm->num_keys * bgm->num_recs);
	key_lens = malloc(sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
	memset(key_lens, 0, sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
	get_key = malloc(sizeof(void *));
	*get_key = NULL;
	get_key_len = malloc(sizeof(int32_t));
	*get_key_len = 0;
	get_value = malloc(sizeof(void *));
	get_value_len = malloc(sizeof(int32_t));
	num_records = 0;

	//Get the index referenced the message
	opendb = _find_opendb_inc_ref(bgm->basem.db_path);
	if (!opendb) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Error retrieving opendb for id: %s", 
		     md->mdhim_rank, bgm->basem.db_path);
		error = MDHIM_ERROR;
		goto respond;
	}
	inc_ref = 1;

	if (bgm->num_keys * bgm->num_recs > MAX_BULK_OPS) {
		mlog(MDHIM_SERVER_CRIT, "Rank: %d - Too many bulk operations requested", 
		     md->mdhim_rank);
		error = MDHIM_ERROR;
		goto respond;
	}

	mlog(MDHIM_SERVER_CRIT, "Rank: %d - Num keys is: %d and num recs is: %d", 
	     md->mdhim_rank, bgm->num_keys, bgm->num_recs);
	gettimeofday(&start, NULL);
	//Iterate through the arrays and get each record
	for (i = 0; i < bgm->num_keys; i++) {
		for (j = 0; j < bgm->num_recs; j++) {
			keys[num_records] = NULL;
			key_lens[num_records] = 0;

			//If we were passed in a key, copy it
			if (!j && bgm->key_lens[i] && bgm->keys[i]) {
				*get_key = malloc(bgm->key_lens[i]);
				memcpy(*get_key, bgm->keys[i], bgm->key_lens[i]);
				*get_key_len = bgm->key_lens[i];
				//If we were not passed a key and this is a next/prev, then return an error
			} else if (!j && (!bgm->key_lens[i] || !bgm->keys[i])
				   && (op ==  MDHIM_GET_NEXT || 
				       op == MDHIM_GET_PREV)) {
				error = MDHIM_ERROR;
				goto respond;
			}

			switch(op) {
				//Get a record from the database
			case MDHIM_GET_FIRST:	
				if (j == 0) {
					keys[num_records] = NULL;
					key_lens[num_records] = sizeof(int32_t);
				}
			case MDHIM_GET_NEXT:	
				if (j && (ret = 
					  opendb->mdhim_store->get_next(opendb->mdhim_store->db_handle, 
								       get_key, get_key_len, 
								       get_value, 
								       get_value_len)) 
				    != MDHIM_SUCCESS) {
					mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get next record", 
					     md->mdhim_rank);
					error = ret;
					key_lens[num_records] = 0;
					value_lens[num_records] = 0;
					goto respond;
				} else if (!j && (ret = 
						  opendb->mdhim_store->get(opendb->mdhim_store->db_handle, 
									  *get_key, *get_key_len, 
									  get_value, 
									  get_value_len))
					   != MDHIM_SUCCESS) {
					error = ret;
					key_lens[num_records] = 0;
					value_lens[num_records] = 0;
					goto respond;
				}
				break;
			case MDHIM_GET_LAST:	
				if (j == 0) {
					keys[num_records] = NULL;
					key_lens[num_records] = sizeof(int32_t);
				}
			case MDHIM_GET_PREV:
				if (j && (ret = 
					  opendb->mdhim_store->get_prev(opendb->mdhim_store->db_handle, 
								       get_key, get_key_len, 
								       get_value, 
								       get_value_len)) 
				    != MDHIM_SUCCESS) {
					mlog(MDHIM_SERVER_DBG, "Rank: %d - Couldn't get prev record", 
					     md->mdhim_rank);
					error = ret;
					key_lens[num_records] = 0;
					value_lens[num_records] = 0;
					goto respond;
				} else if (!j && (ret = 
						  opendb->mdhim_store->get(opendb->mdhim_store->db_handle, 
									  *get_key, *get_key_len, 
									  get_value, 
									  get_value_len))
					   != MDHIM_SUCCESS) {
					error = ret;
					key_lens[num_records] = 0;
					value_lens[num_records] = 0;
					goto respond;
				}
				break;
			default:
				mlog(MDHIM_SERVER_CRIT, "Rank: %d - Invalid operation for bulk get op", 
				     md->mdhim_rank);
				goto respond;
				break;
			}

			keys[num_records] = *get_key;
			key_lens[num_records] = *get_key_len;
			values[num_records] = *get_value;
			value_lens[num_records] = *get_value_len;
			num_records++;
		}
	}

respond:
	if (inc_ref == 1) {
		_find_opendb_dec_ref(bgm->basem.db_path);
	}

	gettimeofday(&end, NULL);
	add_timing(start, end, num_records, md, MDHIM_BULK_GET);

	//Create the response message
	bgrm = malloc(sizeof(struct mdhim_bgetrm_t));
	//Set the type
	bgrm->basem.mtype = MDHIM_RECV_BULK_GET;
	//Set the operation return code as the error
	bgrm->error = error;
	//Set the server's rank
	bgrm->basem.server_rank = md->mdhim_rank;
	//Set the keys and values
	bgrm->keys = keys;
	bgrm->key_lens = key_lens;
	bgrm->values = values;
	bgrm->value_lens = value_lens;
	bgrm->num_keys = num_records;
	//bgrm->basem.index = index->id;
	//bgrm->basem.index_type = index->type;
       
	//Send response
	ret = send_locally_or_remote(md, source, bgrm);

	//Free stuff
	if (source == md->mdhim_rank) {
		/* If this message is not coming from myself, 
		   free the keys and values from the get message */
		mdhim_partial_release_msg(bgm);
	} 

	free(get_key);
	free(get_key_len);
	free(get_value);
	free(get_value_len);

	return MDHIM_SUCCESS;
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *listener_thread(void *data) {	
	//Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
	

	struct mdhim_t *md = (struct mdhim_t *) data;
	void *message;
	int source; //The source of the message
	int ret;
	work_item *item;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	while (1) {
		if (md->shutdown) {
			break;
		}	

		//Clean outstanding sends
		range_server_clean_oreqs(md);

		//Receive messages sent to this server
		ret = receive_rangesrv_work(md, &source, &message);
		if (ret < MDHIM_SUCCESS) {		
			continue;
		}

		//printf("Rank: %d - Received message from rank: %d of type: %d", 
		//     md->mdhim_rank, source, mtype);

        //Create a new work item
		item = malloc(sizeof(work_item));
		memset(item, 0, sizeof(work_item));
		             
		//Set the new buffer to the new item's message
		item->message = message;
		//Set the source in the work item
		item->source = source;
		//Add the new item to the work queue
		range_server_add_work(md, item);
	}

	return NULL;
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
void *worker_thread(void *data) {
	//Mlog statements could cause a deadlock on range_server_stop due to canceling of threads

	struct mdhim_t *md = (struct mdhim_t *) data;
	work_item *item, *item_tmp;
	int mtype;
	int op, num_records, num_keys;

	while (1) {
		if (md->shutdown) {
			break;
		}
		//Lock the work queue mutex
		pthread_mutex_lock(md->mdhim_rs->work_queue_mutex);
		pthread_cleanup_push((void (*)(void *)) pthread_mutex_unlock,
				     (void *) md->mdhim_rs->work_queue_mutex);

		//Wait until there is work to be performed
		if ((item = get_work(md)) == NULL) {
			pthread_cond_wait(md->mdhim_rs->work_ready_cv, md->mdhim_rs->work_queue_mutex);
			item = get_work(md);
		}
	       
		pthread_cleanup_pop(0);
		if (!item) {
			pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);			
			continue;
		}
		pthread_mutex_unlock(md->mdhim_rs->work_queue_mutex);

		//Clean outstanding sends
		range_server_clean_oreqs(md);

		while (item) {
			//Call the appropriate function depending on the message type			
			//Get the message type
			mtype = ((struct mdhim_basem_t *) item->message)->mtype;

//			printf("Rank: %d - Got work item from queue with type: %d" 
//			     " from: %d\n", md->mdhim_rank, mtype, item->source);

			switch(mtype) {
			case MDHIM_OPEN:
				range_server_open(md, item->message,
						  item->source);
				break;
			case MDHIM_CLOSE:
				range_server_close(md, item->message,
						  item->source);
				break;
			case MDHIM_PUT:
				//Pack the put message and pass to range_server_put
				range_server_put(md, 
						 item->message, 
						 item->source);
				break;
			case MDHIM_BULK_PUT:
				//Pack the bulk put message and pass to range_server_put
				range_server_bput(md, 
						  item->message, 
						  item->source);
				break;
			case MDHIM_BULK_GET:
				op = ((struct mdhim_bgetm_t *) item->message)->op;
				num_records = ((struct mdhim_bgetm_t *) item->message)->num_recs;
				num_keys = ((struct mdhim_bgetm_t *) item->message)->num_keys;
				//The client is sending one key, but requesting the retrieval of more than one
				if (num_records > 1 && num_keys == 1) {
					range_server_bget_op(md, 
							     item->message, 
							     item->source, op);
				} else {
					range_server_bget(md, 
							  item->message, 
							  item->source);
				}

				break;
			case MDHIM_DEL:
				range_server_del(md, item->message, item->source);
				break;
			case MDHIM_BULK_DEL:
				range_server_bdel(md, item->message, item->source);
				break;
			case MDHIM_COMMIT:
				range_server_commit(md, item->message, item->source);
				break;		
			default:
				printf("Rank: %d - Got unknown work type: %d" 
				       " from: %d\n", md->mdhim_rank, mtype, item->source);
				break;
			}
			
			item_tmp = item;
			item = item->next;
			free(item_tmp);
		}

		//Clean outstanding sends
		range_server_clean_oreqs(md);				
	}

	return NULL;
}

int range_server_add_oreq(struct mdhim_t *md, MPI_Request *req, void *msg) {
	out_req *oreq;
	out_req *item;

	pthread_mutex_lock(md->mdhim_rs->out_req_mutex);
	item = md->mdhim_rs->out_req_list;
	oreq = malloc(sizeof(out_req));
	oreq->next = NULL;
	oreq->prev = NULL;
	oreq->message = msg;
	oreq->req = req;

	if (!item) {
		md->mdhim_rs->out_req_list = oreq;
		pthread_mutex_unlock(md->mdhim_rs->out_req_mutex);
		return MDHIM_SUCCESS;
	}

	item->prev = oreq;
	oreq->next = item;
	md->mdhim_rs->out_req_list = oreq;
	pthread_mutex_unlock(md->mdhim_rs->out_req_mutex);

	return MDHIM_SUCCESS;	
}

int range_server_clean_oreqs(struct mdhim_t *md) {
	out_req *item;
	out_req *t;
	int ret;
	int flag = 0;
	MPI_Status status;

	pthread_mutex_lock(md->mdhim_rs->out_req_mutex);
	item = md->mdhim_rs->out_req_list;
	while (item) {
		if (!item->req) {
			item = item->next;
			continue;
		}

		pthread_mutex_lock(md->mdhim_comm_lock);
		ret = MPI_Test((MPI_Request *)item->req, &flag, &status); 
		pthread_mutex_unlock(md->mdhim_comm_lock);

		if (!flag) {
			item = item->next;
			continue;
		}
		
		if (item == md->mdhim_rs->out_req_list) {
			md->mdhim_rs->out_req_list = item->next;
			if (item->next) {
				item->next->prev = NULL;
			}
		} else {
			if (item->next) {
				item->next->prev = item->prev;
			}
			if (item->prev) {
				item->prev->next = item->next;
			}
		}

		t = item->next;
		free(item->req);
		if (item->message) {
			free(item->message);
		}

		free(item);
		item = t;
	}

	pthread_mutex_unlock(md->mdhim_rs->out_req_mutex);

	return MDHIM_SUCCESS;
}

/**
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(struct mdhim_t *md, int num_wthreads) {
	int ret;
	int i;

	//Allocate memory for the mdhim_rs_t struct
	md->mdhim_rs = malloc(sizeof(struct mdhim_rs_t));
	if (!md->mdhim_rs) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize variables for printing out timings
	md->mdhim_rs->put_time = 0;
	md->mdhim_rs->get_time = 0;
	md->mdhim_rs->num_put = 0;
	md->mdhim_rs->num_get = 0;
	//Initialize work queue
	md->mdhim_rs->work_queue = malloc(sizeof(work_queue_t));
	md->mdhim_rs->work_queue->head = NULL;
	md->mdhim_rs->work_queue->tail = NULL;
	md->mdhim_rs->num_wthreads = num_wthreads;

	//Initialize the outstanding request list
	md->mdhim_rs->out_req_list = NULL;

	//Initialize work queue mutex
	md->mdhim_rs->work_queue_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->mdhim_rs->work_queue_mutex) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_mutex_init(md->mdhim_rs->work_queue_mutex, NULL)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing work queue mutex", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize out req mutex
	md->mdhim_rs->out_req_mutex = malloc(sizeof(pthread_mutex_t));
	if (!md->mdhim_rs->out_req_mutex) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_mutex_init(md->mdhim_rs->out_req_mutex, NULL)) != 0) {    
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing out req mutex", md->mdhim_rank);
		return MDHIM_ERROR;
	}

	//Initialize the condition variables
	md->mdhim_rs->work_ready_cv = malloc(sizeof(pthread_cond_t));
	if (!md->mdhim_rs->work_ready_cv) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while allocating memory for range server", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	if ((ret = pthread_cond_init(md->mdhim_rs->work_ready_cv, NULL)) != 0) {
		mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
		     "Error while initializing condition variable", 
		     md->mdhim_rank);
		return MDHIM_ERROR;
	}
	
	//Initialize worker threads
	md->mdhim_rs->workers = malloc(sizeof(pthread_t *) * md->mdhim_rs->num_wthreads);
	for (i = 0; i < md->db_opts->num_wthreads; i++) {
		md->mdhim_rs->workers[i] = malloc(sizeof(pthread_t));
		if ((ret = pthread_create(md->mdhim_rs->workers[i], NULL, 
					  worker_thread, (void *) md)) != 0) {    
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
			     "Error while initializing worker thread", 
			     md->mdhim_rank);
			return MDHIM_ERROR;
		}
	}

	//Initialize listener threads
	if ((ret = pthread_create(&md->mdhim_rs->listener, NULL, 
				  listener_thread, (void *) md)) != 0) {
	  mlog(MDHIM_SERVER_CRIT, "MDHIM Rank: %d - " 
	       "Error while initializing listener thread", 
	       md->mdhim_rank);
	  return MDHIM_ERROR;
	}

	return MDHIM_SUCCESS;
}

