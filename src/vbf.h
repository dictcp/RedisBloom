#ifndef RM_VBF_H
#define RM_VBF_H

#include <stdint.h> // uint32_t

#define REDIS_MODULE_TARGET
#ifdef REDIS_MODULE_TARGET 
#include "redismodule.h"
#define VBF_CALLOC(count, size) RedisModule_Calloc(count, size)
#define VBF_FREE(ptr) RedisModule_Free(ptr)
#else
#define VBF_CALLOC(count, size) calloc(count, size)
#define VBF_FREE(ptr) free(ptr)
#endif

typedef struct VBF {
    size_t width;
    size_t depth;
    uint32_t *array;
    size_t counter;
} VBFketch;

typedef struct {
    VBFketch *dest;
    long long numKeys;
    VBFketch **vbfArray;
    long long *weights;
} mergeParams;

/* Creates a new Count-Min Sketch with dimensions of width * depth */
VBFketch *NewVBFketch(size_t width, size_t depth);

/*  Recommends width & depth for expected n different items,
    with probability of an error  - prob and over estimation
    error - overEst (use 1 for max accuracy) */
void VBF_DimFromProb(double overEst, double prob, size_t *width, size_t *depth);

void VBF_Destroy(VBFketch *vbf);

/*  Increases item count in value.
    Value must be a non negative number */
size_t VBF_IncrBy(VBFketch *vbf, const char *item, size_t strlen, size_t value);

/* Returns an estimate counter for item */
size_t VBF_Query(VBFketch *vbf, const char *item, size_t strlen);

/*  Merges multiple VBFketches into a single one.
    All sketches must have identical width and depth.
    dest must be already initialized.
*/
void VBF_Merge(VBFketch *dest, size_t quantity, const VBFketch **src, const long long *weights);
void VBF_MergeParams(mergeParams params);

/* Help function */
void VBF_Print(const VBFketch *vbf);

#endif