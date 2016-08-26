#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "bwt.h"
#include "utils.h"

#define PAGE_SIZE (sysconf(_SC_PAGESIZE))
#define MMAP_ALIGN(x) ((x  + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1))

size_t align_mmap(size_t sz)
{
	size_t pg = sysconf(_SC_PAGESIZE);
	return (sz  + pg - 1) & ~(pg - 1);
}

void bwt_dump_bwt(const char *fn, const bwt_t *bwt)
{
	FILE *fp;
	fp = xopen(fn, "wb");
	fwrite(&bwt->primary, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->L2+1, sizeof(bwtint_t), 4, fp);
	fwrite(bwt->bwt, sizeof(bwtint_t), bwt->bwt_size, fp);
	fclose(fp);
}

void bwt_dump_sa(const char *fn, const bwt_t *bwt)
{
	FILE *fp;
	fp = xopen(fn, "wb");
	fwrite(&bwt->primary, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->L2+1, sizeof(bwtint_t), 4, fp);
	fwrite(&bwt->sa_intv, sizeof(bwtint_t), 1, fp);
	fwrite(&bwt->seq_len, sizeof(bwtint_t), 1, fp);
	fwrite(bwt->sa + 1, sizeof(bwtint_t), bwt->n_sa - 1, fp);
	fclose(fp);
}

void bwt_restore_sa(const char *fn, bwt_t *bwt)
{
	char skipped[256];
	FILE *fp;
	bwtint_t primary;

	fp = xopen(fn, "rb");
	fread(&primary, sizeof(bwtint_t), 1, fp);
	xassert(primary == bwt->primary, "SA-BWT inconsistency: primary is not the same.");
	fread(skipped, sizeof(bwtint_t), 4, fp); // skip
	fread(&bwt->sa_intv, sizeof(bwtint_t), 1, fp);
	fread(&primary, sizeof(bwtint_t), 1, fp);
	xassert(primary == bwt->seq_len, "SA-BWT inconsistency: seq_len is not the same.");

	bwt->n_sa = (bwt->seq_len + bwt->sa_intv) / bwt->sa_intv;
	bwt->sa = (bwtint_t*)calloc(bwt->n_sa, sizeof(bwtint_t));
	bwt->sa[0] = -1;

	fread(bwt->sa + 1, sizeof(bwtint_t), bwt->n_sa - 1, fp);
	fclose(fp);
}

bwt_t *bwt_restore_bwt(const char *fn)
{
	bwt_t *bwt;

	bwt = (bwt_t*)calloc(1, sizeof(bwt_t));

	struct stat sb;

	if (stat(fn, &sb) == -1)
	{
		perror("stat");
		abort();
	}

	bwt->bwt_size = (sb.st_size - sizeof(bwtint_t) * 5) >> 2;
	int fd = open(fn, O_RDONLY);
	if (fd < 0)
	{
		perror("bwt open");
		abort();
	}
	bwt->mmap_bwt_size = align_mmap(sb.st_size);
	bwt->mmap_bwt_addr = mmap(NULL, bwt->mmap_bwt_size, PROT_READ, MAP_SHARED|MAP_POPULATE, fd, 0);
	if (bwt->mmap_bwt_addr == MAP_FAILED)
	{
		perror("bwt mmap");
		abort();
	}

	void *p = bwt->mmap_bwt_addr;

	memcpy(&bwt->primary, p, sizeof(bwtint_t));
	p += sizeof(bwtint_t);

	memcpy(bwt->L2+1, p, sizeof(bwtint_t) * 4);
	p += sizeof(bwtint_t) * 4;

	bwt->bwt = p;

	bwt->seq_len = bwt->L2[4];
	close(fd);
	bwt_gen_cnt_table(bwt);

	return bwt;
}

void bwt_destroy(bwt_t *bwt)
{
	if (bwt == 0) return;
	free(bwt->sa);
	munmap(bwt->mmap_bwt_addr, bwt->mmap_bwt_size);
	free(bwt);
}
