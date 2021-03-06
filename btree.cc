#include <assert.h>
#include "btree.h"
#include <stdio.h>
#include <string.h>

using namespace std;

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  // BTREE_OP_UPDATE
	  rc = b.SetVal(offset,value);
	  if (rc) { return rc; }
	  rc = b.Serialize(buffercache, node);
	  return rc;
	  // return ERROR_UNIMPL;
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  // 1) Initilize x as root
  // 2) While x is not leaf
  // 	a) Find the child of x to traverse next. Let this by y.
  // 	b) If y is not full, change x to point to y
  // 	c) If y is full, split it and change x to point to one of the two parts of y. If k < mid key of y, set x as first part of y. Else, set x as second part of y. When we split y, we move mid key from y to its parent x.
  // 3) Repeat loop 2) until x is a leaf. Insert k to x.
  
  VALUE_T val;
  ERROR_T rc;
  if(Lookup(key, val) == ERROR_NONEXISTENT){
      BTreeNode root;
      root.Unserialize(buffercache, superblock.info.rootnode);
      if (root.info.numkeys == 0) {
	//cout << "Root initialization" << endl;
	BTreeNode leaf(BTREE_LEAF_NODE,
			superblock.info.keysize,
			superblock.info.valuesize,
			buffercache->GetBlockSize());
        

	SIZE_T lhs;
	SIZE_T rhs;
	//initialize two leaves for root
	rc = AllocateNode(lhs);
	if (rc) { return rc; }

	rc = AllocateNode(rhs);
	if (rc) { return rc; }

	leaf.Serialize(buffercache, lhs);
	leaf.Serialize(buffercache, rhs);
	root.info.numkeys++;
	
	//set pointers for left and right leaves
	rc=root.SetKey(0, key);
	if (rc) { return rc; }
	rc=root.SetPtr(0, lhs);
	if (rc) { return rc; }
	rc=root.SetPtr(1, rhs);
	if (rc) { return rc; }
	rc=root.Serialize(buffercache, superblock.info.rootnode);
        if (rc) { return rc; }
      }    
      //cout << "Beginning insert process...Call Insert Helper" << endl;
      //this is the error we want, can now start insert

      //start from root
      rc = InsertHelper(superblock_index+1, key, value);
      if (rc) { return rc; }
  }
  //key already exists
  else { return ERROR_CONFLICT; }
}

 
ERROR_T BTreeIndex::InsertHelper(const SIZE_T &node, const KEY_T &key, const VALUE_T &value)
{
  //cout << "Enter Insert Helper" << endl;
  //cout << "-----------------------" << endl;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  SIZE_T newnode;
  KEY_T splitkey;
  
  rc = b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    // and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
	// conditional below shouldn't have key == testKey
	// because that would be in the next leaf node (not desirable)
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we need to recurse on the ptr immediately previous to 
	// this one, if it exists

	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
         
        //call recursively
        //cout << "Recursive call 1" << endl;
	rc = InsertHelper(ptr,key,value);
        if (rc) { return rc; }

        //check to see if this node is full
        if(NodeFull(ptr)){
          //cout << "Node is full 1" << endl;
          //if it is full, split it
	  rc = SplitNode(ptr, splitkey, newnode);
          if (rc) { return rc; }
          //cout << "Insert key ptr pair into newly split node" << endl;
	  return InsertKeyVal(node, splitkey, VALUE_T(), newnode);
        }
        else { return rc; }
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      //same process as before
      //cout << "b.info.numkeys > 0" << endl;      

      rc=b.GetPtr(b.info.numkeys,ptr);

      if (rc) { return rc; }

      //cout << "Recursive call 2" << endl;
      rc = InsertHelper(ptr,key,value);
      if (rc) { return rc; }


      if(NodeFull(ptr)){ 
       // cout << "Node is full 2" << endl;
        rc = SplitNode(ptr, splitkey, newnode);
        if (rc) { return rc; }
        return InsertKeyVal(node, splitkey, VALUE_T(), newnode);
      }
      else { return rc; }
    } else {
       // cout << "b.info.numkeys is <= 0" << endl;
        return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
   // cout << "BTREE_LEAF_NODE" << endl;
   // cout << "Insert key val pair into leaf node" << endl;
    //simply insert into leaf (we will split later if necessary)
    return InsertKeyVal(node, key, value, 0);
    break;
  default:
   // cout << "We can't be looking at anything other than a root, internal, or leaf" << endl;
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}



bool BTreeIndex::NodeFull(const SIZE_T node){
  
  // Unserialize node pointer
  BTreeNode b;
  b.Unserialize(buffercache, node);
  SIZE_T full;  

  // Switch based on node type
  // Consider root as interior node
  // Interiors, check that total slots are less than 2/3 full
  // Leafs, check that total slots are less than 2/3 full
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
	full = (2 / 3) * b.info.GetNumSlotsAsInterior();
	return (full <= b.info.numkeys);
    case BTREE_LEAF_NODE:
	full = (2 / 3) * b.info.GetNumSlotsAsLeaf();
	return (full <= b.info.numkeys);

  }
  return false;

}

//splits given node into two nodes
ERROR_T BTreeIndex::SplitNode (const SIZE_T node, KEY_T &midkey, SIZE_T &newnode) {
  BTreeNode lhs;
  //just copy for now
  BTreeNode rhs = lhs;

  ERROR_T rc;
 
  //represents number of keys in both left and right nodes
  SIZE_T numLHS;
  SIZE_T numRHS;

  // the left node will represent the first half of the current node
  rc=lhs.Unserialize(buffercache, node);
  if (rc) { return rc; }
  //allocate space for new node
  rc = AllocateNode(newnode);
  if (rc) { return rc; }

  // write this new space for rhs onto the disk
  rc = rhs.Serialize(buffercache, newnode);
  if (rc) { return rc; }

  char *lhsStart;
  char *rhsStart;
 
  //if we are splitting a leaf node
  if(lhs.info.nodetype == BTREE_LEAF_NODE){
    // number of keys in the left and right nodes
    numLHS = (lhs.info.numkeys+2)/2; //we want to add one more to include ptr that will point to rhs (+2/2 works for both even and odd numbers)

    numRHS = (lhs.info.numkeys - numLHS);
 
    //gives numLeft-1th pointer
    rc=lhs.GetKey(numLHS-1, midkey);
    if (rc) { return rc; }

    //gives ith pointer to ith key value pair (leaf)
    lhsStart = lhs.ResolveKeyVal(numLHS);
    rhsStart = rhs.ResolveKeyVal(0);

    memcpy(rhsStart, lhsStart, numRHS*(lhs.info.keysize+lhs.info.valuesize));
  }

  //we're splitting an interior node
  else{

    numLHS = lhs.info.numkeys / 2;
    numRHS = lhs.info.numkeys - numLHS - 1;

    rc=lhs.GetKey(numLHS, midkey);
    if (rc) { return rc; }

    lhsStart = lhs.ResolvePtr(numLHS);
    rhsStart = rhs.ResolvePtr(0);

    memcpy(rhsStart, lhsStart, numRHS*(lhs.info.keysize+lhs.info.valuesize));
  }

  //these nodes have new key counts
  lhs.info.numkeys = numLHS;
  rhs.info.numkeys = numRHS;

  //write back to disk
  rc = lhs.Serialize(buffercache, node);
  if (rc) { return rc; }

  rc = rhs.Serialize(buffercache, newnode);
  if (rc) { return rc; }

  return ERROR_NOERROR;

}


ERROR_T BTreeIndex::InsertKeyVal(const SIZE_T node, const KEY_T &key, const VALUE_T &value, SIZE_T newnode) {
  BTreeNode b;
  KEY_T testKey;
  ERROR_T rc;

  SIZE_T slotsLeft;
  SIZE_T slotSize;

  rc=b.Unserialize(buffercache, node);
  if (rc) { return rc; }
  SIZE_T numkeys = b.info.numkeys;

  switch (b.info.nodetype) {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      //size of key ptr pair
      slotSize = b.info.keysize + sizeof(SIZE_T);
      break;
    case BTREE_LEAF_NODE:
      //size of key value pair
      slotSize = b.info.keysize + b.info.valuesize;
      break;
    default:
      return ERROR_INSANE;
  }

  //we're adding a new key
  b.info.numkeys++;

  if (numkeys > 0) {
    slotsLeft = numkeys;
    for (SIZE_T i=0; i < numkeys; i++) {
      rc = b.GetKey(i, testKey);
      if (rc) { return rc; }

      if (key < testKey) {
        //we're going to move existing data over one
        void *src = b.ResolveKey(i);
	void *dest = b.ResolveKey(i+1);
        //memmove(destination, source, num_bytes)
        //dest = pointer to dest array where content is copies
        //src = pointer to src of data to be copied
	memmove(dest, src, slotsLeft * slotSize);
      }

      //we want it to point to last key
      if (i == (numkeys - 1)) { i = numkeys; }

      //set key no matter what type of node
      rc = b.SetKey(i,key);
      if (rc) { return rc; }
      if (b.info.nodetype == BTREE_LEAF_NODE) {
        //if it's a leaf node, it needs a key val pair
        rc = b.SetVal(i, value);
        if (rc) { return rc; }
      } else {
        //otherwise it's a key ptr pair
        rc = b.SetPtr(i+1, newnode);
        if(rc) { return rc; }
      }
      break;
    }
    slotsLeft--;
  }
  //there was nothing on the node, that's easy 
  else {
    rc = b.SetKey(0, key);
    if (rc) { return rc; }
 
    rc = b.SetVal(0, value);
    if (rc) { return rc; }
  }
  //write back onto disk
  return b.Serialize(buffercache, node);

}


ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T v = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, v);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  BTreeNode superblock;
  ERROR_T rc;
  rc = superblock.Unserialize(buffercache, 0);
  
  if (rc != ERROR_NOERROR) {
    return rc;
  }

  if (superblock.info.nodetype != BTREE_SUPERBLOCK) {
     cout << "Superblock is not first block" << endl;
     return ERROR_NONEXISTENT;
  }
/*
  SIZE_T keySize = superblock.info.keysize;
  SIZE_T valSize = superblock.info.valuesize;
  SIZE_T blockSize = superblock.info.blocksize;
  */
  BTreeNode root;
  rc = root.Unserialize(buffercache, superblock.info.rootnode); 

  if (rc != ERROR_NOERROR) { 
    return rc;
  }

  if (root.info.nodetype != BTREE_ROOT_NODE) { 
    cout << "Root error" << endl;
    return ERROR_NONEXISTENT;
  }
  

  
  return ERROR_NOERROR;
  

}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}




