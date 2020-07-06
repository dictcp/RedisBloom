#ifndef VBF_MODULE_H
#define VBF_MODULE_H

#include "redismodule.h"

#define DEFAULT_WIDTH 2.7
#define DEFAULT_DEPTH 5

#define VBF_ENC_VER 0

int VBFModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif