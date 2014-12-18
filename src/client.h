/*
 * MDHIM TNG
 * 
 * Client specific implementation
 */

#ifndef      __CLIENT_H
#define      __CLIENT_H

#include "messages.h"

struct mdhim_brm_t *client_open(struct mdhim_openm_t **openmsg_list,
				int num_range_svrs);
struct mdhim_brm_t *client_close(struct mdhim_closem_t **closemsg_list,
				int num_range_svrs);
struct mdhim_rm_t *client_put(struct mdhim_t *md, struct mdhim_putm_t *pm);
struct mdhim_brm_t *client_bput(struct mdhim_t *md, struct index_t *index, 
				struct mdhim_bputm_t **bpm_list);
struct mdhim_bgetrm_t *client_bget(struct mdhim_t *md, struct index_t *index, 
				   struct mdhim_bgetm_t **bgm_list);
struct mdhim_bgetrm_t *client_bget_op(struct mdhim_t *md, struct mdhim_getm_t *gm);
struct mdhim_rm_t *client_delete(struct mdhim_t *md, struct mdhim_delm_t *dm);
struct mdhim_brm_t *client_bdelete(struct mdhim_t *md, struct index_t *index, 
				   struct mdhim_bdelm_t **bdm_list);

#endif
