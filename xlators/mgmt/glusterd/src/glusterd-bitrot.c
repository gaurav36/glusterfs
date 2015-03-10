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
        [GF_BITROT_OPTION_TYPE_NONE]            = "none",
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

int32_t
glusterd_check_if_bitrot_trans_enabled (glusterd_volinfo_t *volinfo)
{
        int32_t  ret           = 0;
        int      flag          = _gf_false;

        flag = glusterd_volinfo_get_boolean (volinfo, VKEY_FEATURES_BITROT);
        if (flag == -1) {
                gf_log ("", GF_LOG_ERROR, "failed to get the bitrot status");
                ret = -1;
                goto out;
        }

        if (flag == _gf_false) {
                ret = -1;
                goto out;
        }
        ret = 0;
out:
        return ret;
}

int32_t
glusterd_bitrot_enable (glusterd_volinfo_t *volinfo, char **op_errstr,
                        gf_boolean_t *crawl)
{
        int32_t         ret             = -1;
        char            *bitrot_status  = NULL;
        xlator_t        *this           = NULL;

        this = THIS;
        GF_ASSERT (this);

        GF_VALIDATE_OR_GOTO (this->name, volinfo, out);
        GF_VALIDATE_OR_GOTO (this->name, crawl, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errstr, out);

        if (glusterd_is_volume_started (volinfo) == 0) {
                *op_errstr = gf_strdup ("Volume is stopped, start volume "
                                        "to enable bitrot.");
                ret = -1;
                goto out;
        }

        ret = glusterd_check_if_bitrot_trans_enabled (volinfo);
        if (ret == 0) {
                *op_errstr = gf_strdup ("Bitrot is already enabled");
                ret = -1;
                goto out;
        }

        bitrot_status = gf_strdup ("on");
        if (!bitrot_status) {
                gf_log (this->name, GF_LOG_ERROR, "memory allocation failed");
                ret = -1;
                goto out;
        }

        ret = dict_set_dynstr (volinfo->dict, VKEY_FEATURES_QUOTA,
                               quota_status);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "dict set failed");
                goto out;
        }

        *crawl = _gf_true;

        ret = 0;
out:
        if (ret && op_errstr && !*op_errstr)
                gf_asprintf (op_errstr, "Enabling quota on volume %s has been "
                             "unsuccessful", volinfo->volname);
        return ret;
}

int32_t
glusterd_bitrot_disable (glusterd_volinfo_t *volinfo, char **op_errstr,
                         gf_boolean_t *crawl)
{
        int32_t           ret            = -1;
        int               i              =  0;
        char              *bitrot_status = NULL;
        char              *value         = NULL;
        xlator_t  *this                  = NULL;
        glusterd_conf_t *conf            = NULL;

        GF_VALIDATE_OR_GOTO (this->name, volinfo, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errstr, out);

        ret = glusterd_check_if_bitrot_trans_enabled (volinfo);
        if (ret == -1) {
                *op_errstr = gf_strdup ("Bitrot is already disabled");
                goto out;
        }

        bitrot_status = gf_strdup ("off");
        if (!bitrot_status) {
                gf_log (this->name, GF_LOG_ERROR, "memory allocation failed");
                ret = -1;
                goto out;
        }

        ret = dict_set_dynstr (volinfo->dict, VKEY_FEATURES_BITROT, quota_status);
        if (ret) {
                gf_log (this->name, GF_LOG_ERROR, "dict set failed");
                goto out;
        }

        *crawl = _gf_true;

        (void) glusterd_clean_up_bit_store (volinfo);

        ret = 0;
out:
        if (ret && op_errstr && !*op_errstr)
                gf_asprintf (op_errstr, "Disabling quota on volume %s has been "
                             "unsuccessful", volinfo->volname);
        return ret;
}

static int
glusterd_bitrot_op (int opcode)
{
        int              ret   = -1;
        xlator_t         *this = NULL;
        glusterd_conf_t  *priv = NULL;

        this = THIS;
        GF_ASSERT (this);

        priv = this->private;
        GF_ASSERT (priv);

        switch (opcode) {
        case GF_QUOTA_OPTION_TYPE_ENABLE:
        case GF_QUOTA_OPTION_TYPE_DISABLE:

                /* TO DO: remove comments
                 * stop or start quota service. one this patch
                 * dependent of bitd daemon refactor patch.
                 */
                //if (glusterd_all_volumes_with_quota_stopped ())
               //         ret = glusterd_svc_stop (&(priv->quotad_svc),
                 //                                SIGTERM);
                //else
                 //       ret = priv->quotad_svc.manager (&(priv->quotad_svc),
                //                                        NULL, PROC_START);
                break;
        default:
                ret = 0;
                break;
        }

        return ret;

}
