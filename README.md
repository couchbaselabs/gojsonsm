JSONSM is a high-speed JSON matching library.  Used for performing complex
expression matching against JSON data.

# Goals
- Enable extremely high speed expression matching against arbitrary JSON.

# What We've Accomplished
- Precompile expressions to the most efficient execution-time format.
- Single-Pass complex expression matching
- Zero allocations at match-time (this is really related to high-speed matching)
- N1QL-like parser to allow simplified expression entry.

# Match Tree
The match tree represents the implied schema of a document, as described by
the set of expressions from the top-level expression being compiled.  For
instance, in the case of an expression such as 
`$doc.age > 14 AND $doc.birthday.year != 2018`, it is clear that the document
should have an `age` field, and also a field called `birthday` which is an
object itself, containing a `year` field.  This leads to an implied
structure of something like the following:

- $doc
  - age
  - birthday
    - year

This structure is used to allow us to scan through the JSON, quickly
determining if there is any specific processing that needs to be performed at
any particular node within it.

# Match Tree Operations
Each node within the match tree nodes contain op nodes which represent the
actual actions that need to be taken when that node is reached inside of the
JSON being matched.  These operations represent things such as 'less than',
'greater than', etc...  These operations are assigned a bucket index which
matches up with a slot within the binary tree array.  This allows for the
result of an operation being performed to be quickly applied to the binary
tree for the expression.

# Binary Tree
The binary tree portion of our compiled expressions represents the binary
operators of the source expression.  Each entry in the tree contains a
left and right child node, a link to the entries parent as well as an
operation type expressing how these children are combined together to
produce the node state.  In the case of a NOT expression, the right node
is ignored and the node result is simply the negated value.  Note that
the binary tree consists of two components, the binary tree itself which
is tied to a Match Tree, and then a matching execution state object
which represents the values for a particular instance of a matcher using
it.

# Variables
In addition to the normal operations being attached to exec nodes, there
is an ability to specify a VariableId for a particular exec node.  This
causes a reference to the byte-slice for the value of that node to be
stored. This enables later stages of processing to perform computations
against the stored values, allowing expressions such as
`$doc.x + $doc.y > 100`.

# "After"'s
After's provide a method to perform operations against a set of variables
once we can be sure all the neccessary variables have been parsed.  In
the case of an expression such as `$doc.a.x + $doc.a.y > 0`, you would
see variables being used for the two pieces, and then an `After` node
placed on the `$doc.a` section of our Exec tree telling the Matcher
to perform the match, since after all of that node has been processed,
we can be certain both variables will have been populated if they were
in the document.

# Loops
JSONSM allows various looping conditions to be executed as well.  For
instance, the `ANY IN`, `EVERY IN` or `ANY AND EVERY IN` expression
types.  These kinds of expressions are implemented by scanning through
the JSON array, performing normal binary tree matching with a special
'loop root` in the binary tree.  Within the looping, the results written
to the binary tree only cause result propagation up until the loop root.
Once it reaches here, depeding on which kind of loop is being performed
and the specific result from the loop, the looping is either continued
or cancelled, with a final result for the loop being applied once the
entire loop has completed.

# JSON Parsing
JSON parsing is performed in two separate modes.  The initial mode is
essentially a normal parser.  It looks for the beginning of a JSON
object and then reads the field names according to the standard JSON
specification.  Once a field name has been read, the name is matched
against the Match Tree that was generated to determine if that
particular field contributes to the overall expression.  If it is
determined that the field has no impact on the expression, the parser
switches to a different mode which effectively parses through the
bytes without performing any storage except the very basic state
needed for JSON scanning.  Once the state of the scanner returns
to the object which was initial being read, the next field name is
read and the logic is continued.

# Binary Tree Optimization
The binary tree is implemented as a flat array where the children of
a particular node are guarenteed to come immediately after the parent.
This ensures that a segment of the tree can be quickly reset after a
loop iteration by simply zero'ing a contiguous section of the array.

# String Matching Optimization
In order to improve the performance of string matching, and to avoid
the need to perform allocations during matching, all constant
strings used in the original expression are pre-escaped and stored in
their escaped format.  This allows byte-wise matching of the string
against the bytes in the JSON without the need to perform additional
work.  In the case of more complex string comparisons, special rules
can still be used to perform the comparison on the escaped bytes.

# Matcher State Optimization
The matcher state size is fully calculated during compilation of the
expression.  This allows the matcher state object to be preallocated
once per thread which is executing.  Thanks to the various other
optimizations being performed, all variable-length objects which need
to be stored can be kept as a slice of bytes from the source JSON
rather than needing to allocate any space.

# License
Copyright 2018 Couchbase, Inc. All rights reserved.
