#ifndef __MDHIM_PRIVATE_H
#define __MDHIM_PRIVATE_H

#include "mdhim.h"

struct mdhim_db;
struct mdhim_rm_t *_open_db(struct mdhim_db *db);
struct mdhim_rm_t *_close_db(struct mdhim_db *db);
struct mdhim_rm_t *_commit_db(struct mdhim_db *db);
struct mdhim_rm_t *_put_record(struct mdhim_db *mdb, struct index_t *index,
			       void *key, int key_len,
			       void *value, int value_len);
struct mdhim_brm_t *_create_brm(struct mdhim_rm_t *rm);
void _concat_brm(struct mdhim_brm_t *head, struct mdhim_brm_t *addition);
struct mdhim_brm_t *_bput_records(struct mdhim_db *mdb, struct index_t *index,
				  void **keys, int *key_lens,
				  void **values, int *value_lens, int num_records);
struct mdhim_bgetrm_t *_bget_records(struct mdhim_db *mdb, struct index_t *index,
				     void **keys, int *key_lens,
				     int num_keys, int num_records, int op);
struct mdhim_brm_t *_bdel_records(struct mdhim_db *mdb, struct index_t *index,
				  void **keys, int *key_lens,
				  int num_records);
#endif
