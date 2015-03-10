/*
   Copyright (c) 2011-2012 Red Hat, Inc. <http://www.redhat.com>
   This file is part of GlusterFS.
  
   This file is licensed to you under your choice of the GNU Lesser
   General Public License, version 3 or any later version (LGPLv3 or
   later), or the GNU General Public License, version 2 (GPLv2), in all
   cases as published by the Free Software Foundation.
 */

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "common-utils.h"
#include "cli1-xdr.h"
#include "xdr-generic.h"
#include "glusterd.h"
#include "glusterd-op-sm.h"
#include "glusterd-store.h"
#include "glusterd-utils.h"
#include "glusterd-volgen.h"
#include "run.h"
#include "syscall.h"
#include "byte-order.h"
#include "compat-errno.h"

#include <sys/wait.h>
#include <dlfcn.h>

const char *gd_bitrot_op_list[GF_BITROT_OPTION_TYPE_SCRUB+1] = {
        [GF_BITROT_OPTION_TYPE_ENABLE]          = "enable",
        [GF_BITROT_OPTION_TYPE_DISABLE]         = "disable",
        [GF_BITROT_OPTION_TYPE_SCRUB_THROTTLE]  = "scrub-throttle",
        [GF_BITROT_OPTION_TYPE_SCRUB_FREQ]      = "scrub-frequency",
        [GF_BITROT_OPTION_TYPE_SCRUB]           = "scrub",
};

int
__glusterd_handle_bitrot (rpcsvc_request_t *req)
{
        int32_t                         ret = -1;
        gf_cli_req                      cli_req = {{0,}};
        dict_t                         *dict = NULL;
        glusterd_op_t                   cli_op = GD_OP_BIT_ROT;
        char                           *volname = NULL;
        int32_t                         type = 0;
        char                            msg[2048] = {0,};
        xlator_t                       *this = NULL;
        glusterd_conf_t                *conf = NULL;

        GF_ASSERT (req);

        this = THIS;
        GF_ASSERT (this);

        conf = this->private;
        GF_ASSERT (conf);

        ret = xdr_to_generic (req->msg[0], &cli_req, (xdrproc_t)xdr_gf_cli_req);
        if (ret < 0) {
                req->rpc_err = GARBAGE_ARGS;
                goto out;
        }

        if (cli_req.dict.dict_len) {
                /* Unserialize the dictionary */
                dict  = dict_new ();

                ret = dict_unserialize (cli_req.dict.dict_val,
                                        cli_req.dict.dict_len,
                                        &dict);
                if (ret < 0) {
                        gf_log (this->name, GF_LOG_ERROR, "failed to "
                                "unserialize req-buffer to dictionary");
                        snprintf (msg, sizeof (msg), "Unable to decode the "
                                  "command");
                        goto out;
                } else {
                        dict->extra_stdfree = cli_req.dict.dict_val;
                }
        }

        ret = dict_get_str (dict, "volname", &volname);
        if (ret) {
                snprintf (msg, sizeof (msg), "Unable to get volume name");
                gf_log (this->name, GF_LOG_ERROR, "Unable to get volume name, "
                        "while handling bitrot command");
                goto out;
        }

        ret = dict_get_int32 (dict, "type", &type);
        if (ret) {
                snprintf (msg, sizeof (msg), "Unable to get type of command");
                gf_log (this->name, GF_LOG_ERROR, "Unable to get type of cmd, "
                        "while handling bitrot command");
                goto out;
        }

        if (conf->op_version < GD_OP_VERSION_3_7_0) {
                snprintf (msg, sizeof (msg), "Cannot execute command. The "
                          "cluster is operating at version %d. Bitrot command "
                          "%s is unavailable in this version", conf->op_version,
                          gd_bitrot_op_list[type]);
                ret = -1;
                goto out;
        }

        ret = glusterd_op_begin_synctask (req, GD_OP_BITROT, dict);

out:
        if (ret) {
                if (msg[0] == '\0')
                        snprintf (msg, sizeof (msg), "Bitrot operation failed");
                ret = glusterd_op_send_cli_response (cli_op, ret, 0, req,
                                                     dict, msg);
        }

        return ret;
}

int
glusterd_handle_bitrot (rpcsvc_request_t *req)
{
        return glusterd_big_locked_handler (req, __glusterd_handle_bitrot);
}


