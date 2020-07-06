#include <assert.h> // assert
#include <math.h>   // q, ceil
#include <stdio.h>  // printf
#include <stdlib.h> // malloc

#include "vbf.h"
#include "contrib/murmurhash2.h"

#define max(a,b) \
    ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
       _a > _b ? _a : _b; })

#define BIT64 64
#define VBF_HASH(item, itemlen, i) MurmurHash2(item, itemlen, i)

VBFketch *NewVBFketch(size_t width, size_t depth) {
    assert(width > 0);
    assert(depth > 0);

    VBFketch *vbf = VBF_CALLOC(1, sizeof(VBFketch));

    vbf->width = width;
    vbf->depth = depth;
    vbf->counter = 0;
    vbf->array = VBF_CALLOC(width * depth, sizeof(uint32_t));

    return vbf;
}

void VBF_DimFromProb(double error, double delta, size_t *width, size_t *depth) {
    assert(error > 0 && error < 1);
    assert(delta > 0 && delta < 1);
 
    *width = ceil(2 / error);
    *depth = ceil(log10f(delta) / log10f(0.5));
}

void VBF_Destroy(VBFketch *vbf) {
    assert(vbf);

    VBF_FREE(vbf->array);
    vbf->array = NULL;

    VBF_FREE(vbf);
}

size_t VBF_IncrBy(VBFketch *vbf, const char *item, size_t itemlen, size_t value) {
    assert(vbf);
    assert(item);

    size_t maxCount = 0;

    for (size_t i = 0; i < vbf->depth; ++i) {
        uint32_t hash = VBF_HASH(item, itemlen, i);
        vbf->array[(hash % vbf->width) + (i * vbf->width)] |= value;
    }
    return 0;
}

size_t VBF_Query(VBFketch *vbf, const char *item, size_t itemlen) {
    assert(vbf);
    assert(item);

    size_t temp = 0, res = (size_t)-1;

    for (size_t i = 0; i < vbf->depth; ++i) {
        uint32_t hash = VBF_HASH(item, itemlen, i);
        temp = vbf->array[(hash % vbf->width) + (i * vbf->width)];
        res &= temp;
    }

    return res;
}

void VBF_Merge(VBFketch *dest, size_t quantity, const VBFketch **src, const long long *weights) {
    assert(dest);
    assert(src);
    assert(weights);

    size_t itemCount = 0;
    size_t vbfCount = 0;
    size_t width = dest->width;
    size_t depth = dest->depth;

    for (size_t i = 0; i < depth; ++i) {
        for (size_t j = 0; j < width; ++j) {
            itemCount = 0;
            for (size_t k = 0; k < quantity; ++k) {
                itemCount += src[k]->array[(i * width) + j] * weights[k];
            }
            dest->array[(i * width) + j] = itemCount;
        }
    }

    for (size_t i = 0; i < quantity; ++i) {
        vbfCount += src[i]->counter * weights[i];
    }
    dest->counter = vbfCount;
}

void VBF_MergeParams(mergeParams params) {
    VBF_Merge(params.dest, params.numKeys, (const VBFketch **)params.vbfArray,
              (const long long *)params.weights);
}


/************ used for debugging *******************
void VBF_Print(const VBFketch *vbf) {
    assert(vbf);

    for (int i = 0; i < vbf->depth; ++i) {
        for (int j = 0; j < vbf->width; ++j) {
            printf("%d\t", vbf->array[(i * vbf->width) + j]);
        }
        printf("\n");
    }
    printf("\tCounter is %lu\n", vbf->counter);
} */