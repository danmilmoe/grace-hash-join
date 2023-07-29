#include "Join.hpp"
#include <iostream>
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel, pair<uint, uint> right_rel) {
	// partitions vector
	vector<Bucket> partitions;
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i) {
		partitions.emplace_back(disk);
	}
	// memory page
	Page* page;
	// input
	uint input_id = MEM_SIZE_IN_PAGE - 1;

	// loop through left rel
	for (uint i = left_rel.first; i < left_rel.second; ++i) {
		// load each disk page into a memory page
		mem->loadFromDisk(disk, i, input_id);
		Page* mem_page = mem->mem_page(input_id);
		// loop through each page
		for (uint j = 0; j < mem_page->size(); j++) {
			// get hash value for key of each record
			Record rec = mem_page->get_record(j);
			uint hash_val = rec.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			// add page to partition at corresponding hash value
			page = mem->mem_page(hash_val);
			page->loadRecord(rec);

			// check if page is full
			if (page->full()) {
				// Flush records to disk, add to partitions
				uint disk_page = mem->flushToDisk(disk, hash_val);
				partitions[hash_val].add_left_rel_page(disk_page);
			}
		}
	}

	// add leftover partial buffer pages to partitions
	for (uint i = 0; i < MEM_SIZE_IN_PAGE; ++i) {
		if (!mem->mem_page(i)->empty() && i != input_id) {
			uint disk_page = mem->flushToDisk(disk, i);
			partitions[i].add_left_rel_page(disk_page);
		}
	}

	// reset all pages
	mem->reset();

	// loop through right rel
	for (uint i = right_rel.first; i < right_rel.second; ++i) {
		// load each disk page into a memory page
		mem->loadFromDisk(disk, i, input_id);
		Page* mem_page = mem->mem_page(input_id);
		// loop through each page
		for (uint j = 0; j < mem_page->size(); j++) {
			// get hash value for key of each record
			Record rec = mem_page->get_record(j);
			uint hash_val = rec.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			// add page to partition at corresponding hash value
			page = mem->mem_page(hash_val);
			page->loadRecord(rec);

			// check if page is full
			if (page->full()) {
				// Flush records to disk, add to partitions
				uint disk_page = mem->flushToDisk(disk, hash_val);
				partitions[hash_val].add_right_rel_page(disk_page);
			}
		}
	}

	// add leftover partial buffer pages to partitions
	for (uint i = 0; i < MEM_SIZE_IN_PAGE; ++i) {
		if (!mem->mem_page(i)->empty() && i != input_id) {
			uint disk_page = mem->flushToDisk(disk, i);
			partitions[i].add_right_rel_page(disk_page);
		}
	}
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	vector<uint> disk_pages; // placeholder
	// input & output
	uint input_id = MEM_SIZE_IN_PAGE - 2;
	uint output_id = MEM_SIZE_IN_PAGE - 1;
	mem->reset();

	// for each partition check which partition is smaller for repartition
	vector<uint> left;
	vector<uint> right;
	uint num_left = 0;
	uint num_right = 0;
	// find the overall size of left and right relations
	for (uint i = 0; i < partitions.size(); ++i) {
		num_left += partitions[i].num_left_rel_record;
		num_right += partitions[i].num_right_rel_record;
	}
	for (uint partition_idx = 0; partition_idx < partitions.size(); ++partition_idx) {
		// get vector of indexes of the left and right relations
		left = partitions[partition_idx].get_left_rel();
		right = partitions[partition_idx].get_right_rel();
		if (num_left >= num_right) {
			for (uint k = 0; k < right.size(); ++k) {
				// load input page from disk
				uint id = right[k];
				mem->loadFromDisk(disk, id, input_id);
				// loop through each page
				for (uint mem_idx = 0; mem_idx < mem->mem_page(input_id)->size(); mem_idx++) {
					// get hash value for key of each record
					Record rec = mem->mem_page(input_id)->get_record(mem_idx);
					uint hash_val = rec.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					// add page to partition at corresponding hash value
					mem->mem_page(hash_val)->loadRecord(rec);
				}
			}
			// clear input page
			mem->mem_page(input_id)->reset();
			for (uint left_idx = 0; left_idx < left.size(); ++left_idx) {
				// load input page from disk
				mem->loadFromDisk(disk, left[left_idx], input_id);
				// loop through each page
				for (uint mem_idx = 0; mem_idx < mem->mem_page(input_id)->size(); mem_idx++) {
					// get hash value for key of each record
					Record rec = mem->mem_page(input_id)->get_record(mem_idx);
					uint hash_val = rec.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					// Check if the record matches each in-memory record
					for (uint rec_idx = 0; rec_idx < mem->mem_page(hash_val)->size(); ++rec_idx) {
						if (rec == mem->mem_page(hash_val)->get_record(rec_idx)) {
							// add match to output page
							mem->mem_page(output_id)->loadPair(rec, mem->mem_page(hash_val)->get_record(rec_idx));
							// check if the output page is full
							if (mem->mem_page(output_id)->full()) {
								// write output to disk amd add disk page to vector
								uint disk_page = mem->flushToDisk(disk, output_id);
								disk_pages.emplace_back(disk_page);
							}
						}
					}
				}
			}
		} else {
			for (uint left_idx = 0; left_idx < left.size(); ++left_idx) {
				// load input page from disk
				mem->loadFromDisk(disk, left[left_idx], input_id);
				// loop through each page
				for (uint mem_idx = 0; mem_idx < mem->mem_page(input_id)->size(); mem_idx++) {
					// get hash value for key of each record
					Record rec = mem->mem_page(input_id)->get_record(mem_idx);
					uint hash_val = rec.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					// add page to partition at corresponding hash value
					mem->mem_page(hash_val)->loadRecord(rec);
				}
			}
			// clear input page
			mem->mem_page(input_id)->reset();
			for (uint right_idx = 0; right_idx < right.size(); ++right_idx) {
				// load input page from disk
				mem->loadFromDisk(disk, right[right_idx], input_id);
				// loop through each page
				for (uint mem_idx = 0; mem_idx < mem->mem_page(input_id)->size(); mem_idx++) {
					// get hash value for key of each record
					Record rec = mem->mem_page(input_id)->get_record(mem_idx);
					uint hash_val = rec.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					// Check if the record matches each in-memory record
					for (uint rec_idx = 0; rec_idx < mem->mem_page(hash_val)->size(); ++rec_idx) {
						if (rec == mem->mem_page(hash_val)->get_record(rec_idx)) {
							// add match to output page
							mem->mem_page(output_id)->loadPair(rec, mem->mem_page(hash_val)->get_record(rec_idx));
							// check if the output page is full
							if (mem->mem_page(output_id)->full()) {
								// write output to disk amd add disk page to vector
								uint disk_page = mem->flushToDisk(disk, output_id);
								disk_pages.emplace_back(disk_page);
							}
						}
					}
				}
			}
		}
		// reset memory except output page
		for (uint mem_idx = 0; mem_idx < MEM_SIZE_IN_PAGE - 1; ++mem_idx) {
			mem->mem_page(mem_idx)->reset();
		}
	}
	// add leftover partial output page to disk
	if (!mem->mem_page(output_id)->empty()) {
		uint disk_page = mem->flushToDisk(disk, output_id);
		disk_pages.emplace_back(disk_page);
	}
	return disk_pages;
}
