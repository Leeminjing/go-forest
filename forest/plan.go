package forest

type OperationKind string

const (
	OpAddRoot       OperationKind = "add-root"
	OpNest          OperationKind = "nest"
	OpMove          OperationKind = "move"
	OpUnnest        OperationKind = "unnest"
	OpLink          OperationKind = "link"
	OpBindNest      OperationKind = "bind-nest"
	OpUnlink        OperationKind = "unlink"
	OpRebind        OperationKind = "rebind"
	OpReplace       OperationKind = "replace"
	OpRemove        OperationKind = "remove"
	OpRemoveSubtree OperationKind = "remove-subtree"
)

type Link struct {
	ID         string
	Consumer   TreeID
	Port       PortID
	Provider   TreeID
	Capability CapabilityID
}

// Binding records either an independent Link or explicit Nest assembly.
type Binding struct {
	Link
	Local bool
	Order uint64
}

// Operation values are inert; Apply is the only topology mutation boundary.
type Operation struct {
	Kind         OperationKind
	Tree         TreeID
	Parent       TreeID
	Definition   DefinitionRef
	Binding      Link
	KeepChildren bool
}
type ChangePlan struct {
	BaseGeneration uint64
	Operations     []Operation
}

func AddRoot(id TreeID, d DefinitionRef) Operation {
	return Operation{Kind: OpAddRoot, Tree: id, Definition: d}
}
func Nest(parent, id TreeID, d DefinitionRef) Operation {
	return Operation{Kind: OpNest, Tree: id, Parent: parent, Definition: d}
}
func Move(id, parent TreeID) Operation { return Operation{Kind: OpMove, Tree: id, Parent: parent} }
func Unnest(id TreeID) Operation       { return Operation{Kind: OpUnnest, Tree: id} }
func Connect(link Link) Operation      { return Operation{Kind: OpLink, Binding: link} }
func BindNest(link Link) Operation     { return Operation{Kind: OpBindNest, Binding: link} }
func Unlink(id string) Operation       { return Operation{Kind: OpUnlink, Binding: Link{ID: id}} }

// Rebind preserves a binding's identity, order, and local/link classification.
func Rebind(link Link) Operation { return Operation{Kind: OpRebind, Binding: link} }
func Replace(id TreeID, d DefinitionRef, keepChildren bool) Operation {
	return Operation{Kind: OpReplace, Tree: id, Definition: d, KeepChildren: keepChildren}
}
func Remove(id TreeID) Operation        { return Operation{Kind: OpRemove, Tree: id} }
func RemoveSubtree(id TreeID) Operation { return Operation{Kind: OpRemoveSubtree, Tree: id} }
