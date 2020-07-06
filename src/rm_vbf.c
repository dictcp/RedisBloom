#include <math.h>    // ceil, log10f
#include <stdlib.h>  // malloc
#include <strings.h> // strncasecmp

#include "rmutil/util.h"
#include "version.h"

#include "vbf.h"
#include "rm_vbf.h"

#define INNER_ERROR(x)                                                                             \
    RedisModule_ReplyWithError(ctx, x);                                                            \
    return REDISMODULE_ERR;

RedisModuleType *VBFketchType;

typedef struct {
    const char *key;
    size_t keylen;
    long long value;
} VBFPair;

static int GetVBFKey(RedisModuleCtx *ctx, RedisModuleString *keyName, VBFketch **vbf, int mode) {
    // All using this function should call RedisModule_AutoMemory to prevent memory leak
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        INNER_ERROR("VBF: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != VBFketchType) {
        INNER_ERROR(REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    *vbf = RedisModule_ModuleTypeGetValue(key);
    return REDISMODULE_OK;
}

static int parseCreateArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                           long long *width, long long *depth) {

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(argv[0], &cmdlen);
    if (strcasecmp(cmd, "vbf.initbydim") == 0) {
        if ((RedisModule_StringToLongLong(argv[2], width) != REDISMODULE_OK) || *width < 1) {
            INNER_ERROR("VBF: invalid width");
        }
        if ((RedisModule_StringToLongLong(argv[3], depth) != REDISMODULE_OK) || *depth < 1) {
            INNER_ERROR("VBF: invalid depth");
        }
    } else {
        double overEst = 0, prob = 0;
        if ((RedisModule_StringToDouble(argv[2], &overEst) != REDISMODULE_OK) || overEst <= 0 ||
            overEst >= 1) {
            INNER_ERROR("VBF: invalid overestimation value");
        }
        if ((RedisModule_StringToDouble(argv[3], &prob) != REDISMODULE_OK) ||
            prob <= 0 || prob >= 1) {
            INNER_ERROR("VBF: invalid prob value");
        }
        VBF_DimFromProb(overEst, prob, (size_t *)width, (size_t *)depth);
    }

    return REDISMODULE_OK;
}

int VBFketch_Create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    VBFketch *vbf = NULL;
    long long width = 0, depth = 0;
    RedisModuleString *keyName = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "VBF: key already exists");
    }

    if (parseCreateArgs(ctx, argv, argc, &width, &depth) != REDISMODULE_OK)
        return REDISMODULE_OK;

    vbf = NewVBFketch(width, depth);
    RedisModule_ModuleTypeSetValue(key, VBFketchType, vbf);

    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int parseIncrByArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, VBFPair **pairs,
                           int qty) {
    for (int i = 0; i < qty; ++i) {
        (*pairs)[i].key = RedisModule_StringPtrLen(argv[2 + i * 2], &(*pairs)[i].keylen);
        RedisModule_StringToLongLong(argv[2 + i * 2 + 1], &((*pairs)[i].value));
    }
    return REDISMODULE_OK;
}

int VBFketch_IncrBy(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc % 2) == 1) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
    VBFketch *vbf = NULL;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        INNER_ERROR("VBF: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != VBFketchType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        vbf = RedisModule_ModuleTypeGetValue(key);
    }

    int pairCount = (argc - 2) / 2;
    VBFPair *pairArray = VBF_CALLOC(pairCount, sizeof(VBFPair));
    parseIncrByArgs(ctx, argv, argc, &pairArray, pairCount);
    RedisModule_ReplyWithArray(ctx, pairCount);
    for (int i = 0; i < pairCount; ++i) {
        size_t count = VBF_IncrBy(vbf, pairArray[i].key, pairArray[i].keylen, pairArray[i].value);
        RedisModule_ReplyWithLongLong(ctx, (long long)count);
    }

    VBF_FREE(pairArray);
    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int VBFketch_Query(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    VBFketch *vbf = NULL;
    if (GetVBFKey(ctx, argv[1], &vbf, REDISMODULE_READ) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    int itemCount = argc - 2;
    size_t length = 0;
    RedisModule_ReplyWithArray(ctx, itemCount);
    for (int i = 0; i < itemCount; ++i) {
        const char *str = RedisModule_StringPtrLen(argv[2 + i], &length);
        RedisModule_ReplyWithLongLong(ctx, VBF_Query(vbf, str, length));
    }

    return REDISMODULE_OK;
}

static int parseMergeArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          mergeParams *params) {
    long long numKeys = params->numKeys;
    int pos = RMUtil_ArgIndex("WEIGHTS", argv, argc);
    if (pos < 0) {
        if (numKeys != argc - 3) {
            INNER_ERROR("VBF: wrong number of keys");
        }
    } else {
        if ((pos != 3 + numKeys) && (argc != 4 + numKeys * 2)) {
            INNER_ERROR("VBF: wrong number of keys/weights");
        }
    }

    if (GetVBFKey(ctx, argv[1], &(params->dest), REDISMODULE_READ | REDISMODULE_WRITE) !=
        REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    size_t width = params->dest->width;
    size_t depth = params->dest->depth;

    for (int i = 0; i < numKeys; ++i) {
        if (pos == -1) {
            params->weights[i] = 1;
        } else if (RedisModule_StringToLongLong(argv[3 + numKeys + 1 + i], &(params->weights[i])) !=
                   REDISMODULE_OK) {
            INNER_ERROR("VBF: invalid weight value");
        }
        if (GetVBFKey(ctx, argv[3 + i], &(params->vbfArray[i]), REDISMODULE_READ) !=
            REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
        if (params->vbfArray[i]->width != width || params->vbfArray[i]->depth != depth) {
            INNER_ERROR("VBF: width/depth is not equal");
        }
    }

    return REDISMODULE_OK;
}

int VBFketch_Merge(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    mergeParams params = {0};
    if (RedisModule_StringToLongLong(argv[2], &(params.numKeys)) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "VBF: invalid numkeys");
    }

    params.vbfArray = VBF_CALLOC(params.numKeys, sizeof(VBFketch *));
    params.weights = VBF_CALLOC(params.numKeys, sizeof(long long));

    if (parseMergeArgs(ctx, argv, argc, &params) != REDISMODULE_OK) {
        VBF_FREE(params.vbfArray);
        VBF_FREE(params.weights);
        return REDISMODULE_OK;
    }

    VBF_MergeParams(params);

    VBF_FREE(params.vbfArray);
    VBF_FREE(params.weights);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int VBFKetch_Info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    VBFketch *vbf = NULL;
    if (GetVBFKey(ctx, argv[1], &vbf, REDISMODULE_READ) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 3 * 2);
    RedisModule_ReplyWithSimpleString(ctx, "width");
    RedisModule_ReplyWithLongLong(ctx, vbf->width);
    RedisModule_ReplyWithSimpleString(ctx, "depth");
    RedisModule_ReplyWithLongLong(ctx, vbf->depth);
    RedisModule_ReplyWithSimpleString(ctx, "count");
    RedisModule_ReplyWithLongLong(ctx, vbf->counter);

    return REDISMODULE_OK;
}

void VBFRdbSave(RedisModuleIO *io, void *obj) {
    VBFketch *vbf = obj;
    RedisModule_SaveUnsigned(io, vbf->width);
    RedisModule_SaveUnsigned(io, vbf->depth);
    RedisModule_SaveUnsigned(io, vbf->counter);
    RedisModule_SaveStringBuffer(io, (const char *)vbf->array,
                                 vbf->width * vbf->depth * sizeof(uint32_t));
}

void *VBFRdbLoad(RedisModuleIO *io, int encver) {
    if (encver > VBF_ENC_VER) {
        return NULL;
    }

    VBFketch *vbf = VBF_CALLOC(1, sizeof(VBFketch));
    vbf->width = RedisModule_LoadUnsigned(io);
    vbf->depth = RedisModule_LoadUnsigned(io);
    vbf->counter = RedisModule_LoadUnsigned(io);
    size_t length = vbf->width * vbf->depth * sizeof(size_t);
    vbf->array = (uint32_t *)RedisModule_LoadStringBuffer(io, &length);

    return vbf;
}

void VBFFree(void *value) { VBF_Destroy(value); }

size_t VBFMemUsage(const void *value) {
    VBFketch *vbf = (VBFketch *)value;
    return sizeof(vbf) + vbf->width * vbf->depth * sizeof(size_t);
}

int VBFModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // TODO: add option to set defaults from command line and in program
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = VBFRdbLoad,
                                 .rdb_save = VBFRdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .mem_usage = VBFMemUsage,
                                 .free = VBFFree};

    VBFketchType = RedisModule_CreateDataType(ctx, "VBFk-TYPE", VBF_ENC_VER, &tm);
    if (VBFketchType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "vbf.initbydim", VBFketch_Create);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "vbf.initbyprob", VBFketch_Create);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "vbf.incrby", VBFketch_IncrBy);
    RMUtil_RegisterReadCmd(ctx, "vbf.query", VBFketch_Query);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "vbf.merge", VBFketch_Merge);
    RMUtil_RegisterReadCmd(ctx, "vbf.info", VBFKetch_Info);

    return REDISMODULE_OK;
}