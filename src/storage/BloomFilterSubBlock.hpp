/**
 * Author: Saket Saurabh
 */

#ifndef QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_

#include <cstddef>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;
class BloomFilterSubBlockDescription;
class Predicate;
class TupleIdSequence;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief SubBlock which defines a bloom filter for the tuples
 *        in TupleStorageSubBlock (within the same StorageBlock).
 **/
class BloomFilterSubBlock {
 public:
  /**
   * @brief Constructor.
   *
   * @param tuple_store The TupleStorageSubBlock whose contents are hashed by
   *                    this BloomFilterSubBlock.
   * @param description A description containing any parameters needed to
   *        construct this SubBlock
   *        Implementation-specific parameters are defined as extensions in
   *        StorageBlockLayout.proto.
   * @param new_block Whether this is a newly-created block.
   * @param sub_block_memory The memory slot to use for the block's contents.
   * @param sub_block_memory_size The size of the memory slot in bytes.
   * @exception BlockMemoryTooSmall This TupleStorageSubBlock hasn't been
   *            provided enough memory to store metadata.
   **/
  BloomFilterSubBlock(const CatalogRelation &relation,
		        const TupleStorageSubBlock &tuple_store,
                const BloomFilterSubBlockDescription &description,
                const bool new_block,
                void *sub_block_memory,
                const std::size_t sub_block_memory_size)
                : relation_(relation),
				  tuple_store_(tuple_store),
				  description_(description),
				  sub_block_memory_(sub_block_memory),
				  sub_block_memory_size_(sub_block_memory_size) {
  }

  /**
   * @brief Virtual destructor
   **/
  virtual ~BloomFilterSubBlock() {
  }

  /**
   * @brief Identify the type of this BloomFilterSubBlock.
   *
   * @return This BloomFilterSubBlock's type.
   **/
  virtual BloomFilterSubBlockType getBloomFilterSubBlockType() const = 0;

  /**
   * @brief Add an entry to this bloom filter
   * @note Implementations should access the necessary attribute values via
   *       parent_'s TupleStorageSubBlock.
   *
   * @param tuple The ID of the tuple to add.
   * @return True if entry was successfully added, false if not
   **/
  virtual bool addEntry(const Tuple &tuple) = 0;

  /**
   * @brief Use this bloom filter to check (possibly a superset of) tuples matching a
   *        particular predicate.
   *
   * @param predicate The predicate to match.
   * @return a bool which indicates whether the predicate matched or not
   **/
  virtual bool getMatchesForPredicate(const Predicate *predicate) const = 0;

  /**
   * @brief Rebuild this bloom filter from scratch.
   *
   * @return True if the bloom filter was successfully rebuilt, false if not
   **/
  virtual bool rebuild() = 0;

 protected:
  const CatalogRelation &relation_;
  const TupleStorageSubBlock &tuple_store_;
  const BloomFilterSubBlockDescription &description_;
  void *sub_block_memory_;
  const std::size_t sub_block_memory_size_;

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFilterSubBlock);
};


class DefaultBloomFilterSubBlock : public BloomFilterSubBlock {
 public:
  DefaultBloomFilterSubBlock(
		  const CatalogRelation &relation,
		  const TupleStorageSubBlock &tuple_store,
	      const BloomFilterSubBlockDescription &description,
		  const bool new_block,
		  void *sub_block_memory,
		  const std::size_t sub_block_memory_size)
 	 	  : BloomFilterSubBlock(relation,
 	 			  	  	  	  	tuple_store,
 	 			  	  	  	  	description,
								new_block,
								sub_block_memory,
								sub_block_memory_size) {} ;

  ~DefaultBloomFilterSubBlock() {
  }

  BloomFilterSubBlockType getBloomFilterSubBlockType() const {
	  return kDefault;
  }

  static std::size_t EstimateBytesForTuples(const CatalogRelation &relation,
                                             const TupleStorageSubBlockDescription &description) {
	  return 64; // TODO: remove this magic number, and find a proper place to initialize this
  }

  bool rebuild() {
	  return true;
  }

  bool addEntry(const Tuple &tuple) {
	  return true;
  }

  bool getMatchesForPredicate(const Predicate *predicate) const {
	  bool flipCoin = ((rand() % RAND_MAX) % 2) == 0 ? true : false;
	  return flipCoin;
  }


};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_
