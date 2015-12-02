/**
 * Author: Saket Saurabh
 */

#ifndef QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_

#include <cstddef>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Tuple.hpp"
#include "utility/Macros.hpp"
#include "utility/BloomFilter.hpp"

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
								sub_block_memory_size) {

	  // initialize the bloom filters and store them in the sub_block_memory
	  bloom_filter_params_.reset(getBloomFilterConfig());

	  // number of bytes taken by bloom filter per attribute
	  bloom_filter_size_ = bloom_filter_params_->optimal_parameters.table_size / bits_per_char;


	  CatalogRelation::const_iterator attr_it;
	  void* bloom_filter_addr = sub_block_memory_;
	  bloom_filter_data_.reset(static_cast<unsigned char*>(bloom_filter_addr));

	  // allocate space for bloom_filter_data_
	  for (attr_it = relation.begin(); attr_it != relation.end(); ++attr_it) {
		  bloom_filter_addr = (static_cast<unsigned char*>(bloom_filter_addr) + bloom_filter_size_);
	  }


	  // allocate space for bloom_filters_
	  bloom_filters_.reset(static_cast<BloomFilter*>(bloom_filter_addr));
	  unsigned int i = 0;
	  for (attr_it = relation.begin(); attr_it != relation.end(); ++attr_it, ++i) {
		  ScopedPtr<BloomFilter> bloomFilter(new BloomFilter(*bloom_filter_params_, bloom_filter_data_.get() + i*bloom_filter_size_
															  ));
		  memcpy(bloom_filter_addr, bloomFilter.get(), sizeof(*bloomFilter));
		  bloom_filter_addr = (static_cast<char*>(bloom_filter_addr) + sizeof(BloomFilter));
	  }

  } ;

  ~DefaultBloomFilterSubBlock() {
	  // bloom_filters_ and bloom_filter_data_ were stored inside sub_memory_block_
	  // hence they will be freed by the StorageBlock
	  bloom_filters_.release();
	  bloom_filter_data_.release();
  }

  BloomFilterSubBlockType getBloomFilterSubBlockType() const {
	  return kDefault;
  }

  static std::size_t EstimateBytesForTuples(const CatalogRelation &relation,
                                             const TupleStorageSubBlockDescription &description) {

	  // initialize bloom filter parameters object
	  ScopedPtr<BloomParameters> bloom_filter_params;
	  bloom_filter_params.reset(getBloomFilterConfig());

	  // number of bytes taken by bloom filter per attribute
	  std::size_t bloom_filter_size = bloom_filter_params->optimal_parameters.table_size / bits_per_char;

	  size_t total_size = 0;
	  size_t size_per_attribute = bloom_filter_size + sizeof(BloomFilter);
	  CatalogRelation::const_iterator attr_it;
	  for (attr_it = relation.begin(); attr_it != relation.end(); ++attr_it) {
		  total_size += size_per_attribute;
	  }
	  return total_size;
  }

  bool rebuild() {
	  return true;
  }

  bool addEntry(const Tuple &tuple) {
	  Tuple::const_iterator attr_it;
	  int bloom_filter_id = 0;
	  for (attr_it = tuple.begin(); attr_it != tuple.end(); ++attr_it, ++bloom_filter_id) {
		  if (!attr_it->isNull()) {
			  ScopedPtr<char> attr_data_ptr(new char[attr_it->getInstanceByteLength()]);
			  attr_it->copyInto(static_cast<void*>(attr_data_ptr.get()));
			  bloom_filters_.get()[bloom_filter_id].insert(attr_data_ptr.get(),
					  	  	  	  	  	  	  	  	 attr_it->getInstanceByteLength());
		  }
	  }
	  return true;
  }

  bool getMatchesForPredicate(const Predicate *predicate) const {
	  if (predicate->getPredicateType() == predicate->kComparison) {

		  ScopedPtr<Predicate> predicate_ptr(predicate->clone());
		  ComparisonPredicate *comparison_predicate =
				  static_cast<ComparisonPredicate*>(predicate_ptr.get());

		  if (comparison_predicate->getComparison().getComparisonID()
				  == comparison_predicate->getComparison().kEqual
				       && comparison_predicate->getRightOperand().hasStaticValue()) {

			  ScopedPtr<Scalar> left_operand_ptr(comparison_predicate->getLeftOperand().clone());
			  ScopedPtr<Scalar> right_operand_ptr(comparison_predicate->getRightOperand().clone());
			  ScalarAttribute *attr = static_cast<ScalarAttribute*>(left_operand_ptr.get());
			  ScalarLiteral *literal = static_cast<ScalarLiteral*>(right_operand_ptr.get());
			  int attr_id = attr->getAttribute().getID();

			  ScopedPtr<char> attr_data_ptr(new char[literal->getStaticValue().getInstanceByteLength()]);
			  literal->getStaticValue().copyInto(static_cast<void*>(attr_data_ptr.get()));

			  bool isContained = bloom_filters_.get()[attr_id].contains(attr_data_ptr.get(),
					                literal->getStaticValue().getInstanceByteLength());

			  return isContained;
		  }
	  }

	  return true; // default
  }

  // configure Default bloom filter
  static BloomParameters* getBloomFilterConfig() {
	  ScopedPtr<BloomParameters> bloomParams(new BloomParameters());
	  bloomParams->minimum_number_of_hashes = 10;
	  bloomParams->maximum_number_of_hashes = 20;
	  bloomParams->minimum_size = 8000;
	  bloomParams->maximum_size = 8000000;
	  bloomParams->projected_element_count = 1000000;
	  bloomParams->false_positive_probability = 0.01;
	  bloomParams->compute_optimal_parameters();
	  return bloomParams.release();
  }

 protected:

  ScopedPtr<BloomFilter> bloom_filters_;
  ScopedPtr<BloomParameters> bloom_filter_params_;
  ScopedPtr<unsigned char> bloom_filter_data_;
  std::size_t bloom_filter_size_;		// number of bytes taken by bloom filter per attribute

};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BLOOM_FILTER_SUB_BLOCK_HPP_
