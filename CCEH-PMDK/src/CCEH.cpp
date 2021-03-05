#include <iostream>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <vector>
#include "src/CCEH.h"
#include "src/hash.h"

int crashed = 0;

struct val {
  int data;
};

PMEMobjpool * create_pmem(const char *path, const char *layout,
        size_t poolsize, mode_t mode){
  PMEMobjpool *pop = pmemobj_create(path, layout, poolsize, mode);
  TOID_DECLARE(struct val, 123);
  TOID(struct val) v;
  POBJ_ALLOC(pop, &v, struct val, sizeof(struct val), NULL, NULL);
  D_RW(v)->data = 1;
  return pop;
}
PMEMobjpool * open_pmem(const char *path, const char *layout){
  return pmemobj_open(path, layout);
}

int Segment::Insert(PMEMobjpool* pop, Key_t& key, Value_t value, size_t loc, size_t key_hash){
#ifdef INPLACE
    if(sema == -1) {/*printf("sema is 2\n");*/return 2;}
    if((key_hash >> (8*sizeof(size_t)-local_depth)) != pattern) {/*printf("hash is %d pattern is %d\n",
     (key_hash >> (8*sizeof(size_t)-local_depth)), pattern);*/return 2;}
    auto lock = sema;
    int ret = 1;
    while(!CAS(&sema, &lock, lock+1)){
	lock = sema;
    }
    Key_t LOCK = INVALID;
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto slot = (loc+i) % kNumSlot;
	auto _key = pair[slot].key;
	if((h(&pair[slot].key, sizeof(Key_t)) >> (8*sizeof(size_t)-local_depth)) != pattern){
	    CAS(&pair[slot].key, &_key, INVALID);
	}
	if(CAS(&pair[slot].key, &LOCK, SENTINEL)){
	    pair[slot].value = value;
	    pair[slot].key = key;
	    pmemobj_persist(pop, (char*)&pair[slot], sizeof(Pair));
	    ret = 0;
	    break;
	}
	else{
	    LOCK = INVALID;
	}
    }

    lock = sema;
    while(!CAS(&sema, &lock, lock-1)){
	lock = sema;
    }

    return ret;
#else
    if(sema == -1) {/*printf("sema is 2\n");*/ return 2;}
    if((key_hash >> (8*sizeof(size_t)-local_depth)) != pattern) {printf("hash is %d pattern is %d\n",
     (key_hash >> (8*sizeof(size_t)-local_depth)), pattern);return 2;}
    if((key_hash >> (8*sizeof(size_t)-local_depth)) != pattern) return 2;
    auto lock = sema;
    int ret = 1;
    while(!CAS(&sema, &lock, lock+1)){
	lock = sema;
    }
    Key_t LOCK = INVALID;
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto slot = (loc+i) % kNumSlot;
	if(CAS(&pair[slot].key, &LOCK, SENTINEL)){
	    pair[slot].value = value;
	    pair[slot].key = key;
	    pmemobj_persist(pop, (char*)&pair[slot], sizeof(Pair));
	    ret = 0;
	    break;
	}
	else{
	    LOCK = INVALID;
	}
    }

    lock = sema;
    while(!CAS(&sema, &lock, lock-1)){
	lock = sema;
    }

    return ret;
#endif
}

void Segment::Insert4split(Key_t& key, Value_t value, size_t loc){
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto slot = (loc+i) % kNumSlot;
	if(pair[slot].key == INVALID){
	    pair[slot].key = key;
	    pair[slot].value = value;
	    return;
	}
    }
}

TOID(struct Segment)* Segment::Split(PMEMobjpool* pop){
    using namespace std;
    int64_t lock = 0;
    if(!CAS(&sema, &lock, -1)) return nullptr;

#ifdef INPLACE
    TOID(struct Segment)* split = (TOID(struct Segment)*)malloc(sizeof(TOID(struct Segment))*2);
    split[0] = pmemobj_oid(this);
    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[1])->initSegment(local_depth+1);

    for(int i=0; i<kNumSlot; ++i){
	auto key_hash = h(&pair[i].key, sizeof(Key_t));
	if(key_hash & ((size_t)1 << ((sizeof(key_t)*8 - local_depth - 1)))){
	    D_RW(split[1])->Insert4split(pair[i].key, pair[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
	}
    }

    pmemobj_persist(pop, (char*)&split[1], sizeof(struct Segment));
    local_depth = local_depth+1;
    //printf("increment local depth to be %d in split\n", local_depth);
    pmemobj_persist(pop, (char*)&local_depth, sizeof(size_t));

    return split;
#else
    TOID(struct Segment)* split = (TOID(struct Segment)*)malloc(sizeof(TOID(struct Segment))*2);
    POBJ_ALLOC(pop, &split[0], struct Segment, sizeof(struct Segment), NULL, NULL);
    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[0])->initSegment(local_depth+1);
    D_RW(split[1])->initSegment(local_depth+1);

    for(int i=0; i<kNumSlot; ++i){
	auto key_hash = h(&pair[i].key, sizeof(Key_t));
	if(key_hash & ((size_t)1 << ((sizeof(key_t)*8 - local_depth - 1)))){
	    D_RW(split[1])->Insert4split(pair[i].key, pair[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
	}
	else{
	    D_RW(split[0])->Insert4split(pair[i].key, pair[i].value, (key_hash & kMask)*kNumPairPerCacheLine);
	}
    }

    pmemobj_persist(pop, (char*)&split[0], sizeof(struct Segment));
    pmemobj_persist(pop, (char*)&split[1], sizeof(struct Segment));

    return split;
#endif
}


void CCEH::initCCEH(PMEMobjpool* pop){
    //pmemobj_create("/mnt/pmem/b.pm", "CCEH", PMEMOBJ_MIN_POOL, 0666);
    if((uint64_t)pop < 10){
      create_pmem("/mnt/pmem/b.pm", "CCEH", PMEMOBJ_MIN_POOL, 0666);
      open_pmem("/mnt/pmem/b.pm", "CCEH");
    }
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory();
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    for(int i=0; i<D_RO(dir)->capacity; ++i){
	POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
	D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment();
	D_RW(D_RW(D_RW(dir)->segment)[i])->pattern = i;
    }
}

void CCEH::initCCEH(PMEMobjpool* pop, size_t initCap){
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory(static_cast<size_t>(log2(initCap)));
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    printf("dir cap(%lld), each segment size(%d)\n", D_RO(dir)->capacity, sizeof(struct Segment));
    for(int i=0; i<D_RO(dir)->capacity; ++i){
	POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
	D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment(static_cast<size_t>(log2(initCap)));
	D_RW(D_RW(D_RW(dir)->segment)[i])->pattern = i;
    }
}
 
void CCEH::Insert(PMEMobjpool* pop, Key_t& key, Value_t value){
	if(crashed){
		printf("crashed\n");
		return;
        }
STARTOVER:
    auto key_hash = h(&key, sizeof(Key_t));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto x = (key_hash >> (8*sizeof(size_t)-D_RO(dir)->depth));
    //printf("depth at insertion is %d %lu\n", D_RO(dir)->depth, x);
    auto target = D_RO(D_RO(dir)->segment)[x];
    auto ret = D_RW(target)->Insert(pop, key, value, y, key_hash);
    //if(ret != 1)
    //	printf("ret is %d\n", ret);

    if(ret == 1){
	//printf("enter here\n");
	pmemobj_persist(pop, D_RW(target), sizeof(struct Segment));
	TOID(struct Segment)* s = D_RW(target)->Split(pop);
	if(s == nullptr){
	    // another thread is doing split
	    goto RETRY;
	}

	D_RW(s[0])->pattern = (key_hash >> (8*sizeof(size_t)-D_RO(s[0])->local_depth+1)) << 1;
	D_RW(s[1])->pattern = ((key_hash >> (8*sizeof(size_t)-D_RO(s[1])->local_depth+1)) << 1) + 1;
	//printf("pattern is %d %d\n", D_RW(s[0])->pattern, D_RW(s[1])->pattern);
	//printf("local depth is %d %d\n", D_RO(s[0])->local_depth, D_RO(s[1])->local_depth);
	//printf("Directory depth here is %d\n", D_RO(dir)->depth);
	// Directory Management
	while(!D_RW(dir)->Acquire()){
	    asm("nop");
	}

	{ // CRITICAL SECTION - directory update
#ifdef INPLACE
	    if(D_RO(target)->local_depth-1 < D_RO(dir)->depth)
#else
	    if(D_RO(target)->local_depth < D_RO(dir)->depth)
#endif
	    {
		unsigned depth_diff = D_RO(dir)->depth - D_RW(s[0])->local_depth;
		if(depth_diff == 0){
		    if(x%2 == 0){
			D_RW(D_RW(dir)->segment)[x+1] = s[1];
#ifdef INPLACE
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x+1], 8);
#else
			D_RW(D_RW(dir)->segment)[x] = s[0];
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], 16);
#endif
		    }
		    else{
			D_RW(D_RW(dir)->segment)[x] = s[1];
#ifdef INPLACE
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], 8);
#else
			D_RW(D_RW(dir)->segment)[x-1] = s[0];
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x-1], 16);
#endif
		    }
		}
		else{
		    int chunk_size = pow(2, D_RO(dir)->depth - (D_RO(s[0])->local_depth - 1));
		    x = x - (x%chunk_size);
		    for(int i=0; i<chunk_size/2; ++i){
			D_RW(D_RW(dir)->segment)[x+chunk_size/2+1] = s[1];
		    }
		    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x+chunk_size/2], sizeof(void*)*chunk_size/2);
#ifndef INPLACE
		    for(int i=0; i<chunk_size/2; ++i){
			D_RW(D_RW(dir)->segment)[x+i] = s[0];
		    }
		    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(void*)*chunk_size/2);
#endif
		}

		while(!D_RW(dir)->Release()){
		    asm("nop");
		}
	    }
	    else{ // directory doubling
		printf("directory doubling\n");
		auto dir_old = dir;
		TOID_ARRAY(TOID(struct Segment)) d = D_RO(dir)->segment;
		TOID(struct Directory) _dir;
		POBJ_ALLOC(pop, &_dir, struct Directory, sizeof(struct Directory), NULL, NULL);
		D_RW(_dir)->initDirectory(D_RO(dir)->depth+1);
		//crashed = 1;
		//return;
		POBJ_ALLOC(pop, &D_RW(_dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(_dir)->capacity, NULL, NULL);
		for(int i=0; i<D_RO(_dir)->capacity; ++i){
		    POBJ_ALLOC(pop, &D_RO(D_RO(_dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
		    D_RW(D_RW(D_RW(_dir)->segment)[i])->initSegment(D_RO(_dir)->capacity);
		}
		for(int i=0; i<D_RO(dir)->capacity; ++i){
		    if(i == x){
			D_RW(D_RW(_dir)->segment)[2*i] = s[0];
			D_RW(D_RW(_dir)->segment)[2*i+1] = s[1];
		    }
		    else{
			D_RW(D_RW(_dir)->segment)[2*i] = D_RO(d)[i];
			D_RW(D_RW(_dir)->segment)[2*i+1] = D_RO(d)[i];
		    }
		}

		pmemobj_persist(pop, (char*)&D_RO(D_RO(_dir)->segment)[0], sizeof(struct Segment*)*D_RO(_dir)->capacity);
		pmemobj_persist(pop, (char*)&_dir, sizeof(struct Directory));
		//This needs to be uncommented for things to work
		if(crashed != 1){
		  crashed = 1;
		  D_RW(s[0])->sema = 0;
		  //local_depth = local_depth - 1;
	//D_RW(target)->local_depth =  D_RW(target)->local_depth - 1;
		  //D_RW(s[0])->local_depth = D_RW(s[0])->local_depth -1 ;
		  //D_RW(s[1])->local_depth = D_RW(s[1])->local_depth -1 ;
	pmemobj_persist(pop, D_RW(target), sizeof(struct Segment));

	printf("address is %p\n", D_RW(target));
	//D_RW(target)->pattern = ((key_hash >> (8*sizeof(size_t)-D_RO(s[0])->local_depth+1)) << 1) + 1;
		//D_RW(s[0])->pattern = (key_hash >> (8*sizeof(size_t)-D_RO(s[0])->local_depth+1)) << 1;
		//D_RW(s[1])->pattern = ((key_hash >> (8*sizeof(size_t)-D_RO(s[1])->local_depth+1)) << 1) + 1;
		//goto RETRY;

		D_RW(dir)->Release();
		  return;
		}
		dir = _dir;
		pmemobj_persist(pop, (char*)&dir, sizeof(void*));
		POBJ_FREE(&dir_old);
	    }
#ifdef INPLACE
	    D_RW(s[0])->sema = 0;
#endif
        } // End of critical section
        goto RETRY;
    }
    else if(ret == 2){
        goto STARTOVER;
    }
}

bool CCEH::Delete(Key_t& key){
    return false;
}

Value_t CCEH::Get(Key_t& key){
    auto key_hash = h(&key, sizeof(key));
    auto x = (key_hash >> (8*sizeof(key_hash)-D_RO(dir)->depth));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

    auto dir_ = D_RO(D_RO(dir)->segment)[x];

#ifdef INPLACE
    auto sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
    while(!CAS(&D_RW(D_RW(D_RW(dir)->segment)[x])->sema, &sema, sema+1)){
	sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
    }
#endif

    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto slot = (y+i) % Segment::kNumSlot;
	if(D_RO(dir_)->pair[slot].key == key){
#ifdef INPLACE
	    sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
	    while(!CAS(&D_RW(D_RW(D_RW(dir)->segment)[x])->sema, &sema, sema-1)){
		sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
	    }
#endif
	    return D_RO(dir_)->pair[slot].value;
	}
    }

#ifdef INPLACE
    sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
    while(!CAS(&D_RW(D_RW(D_RW(dir)->segment)[x])->sema, &sema, sema-1)){
	sema = D_RO(D_RO(D_RO(dir)->segment)[x])->sema;
    }
#endif
    return NONE;
}
