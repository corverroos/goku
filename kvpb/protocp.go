package kvpb

import "github.com/corverroos/goku"

func FromProto(in *KV) goku.KV {
	return goku.KV{
		Key:        in.Key,
		Value:      in.Value,
		Version:    in.Version,
		CreatedRef: in.CreatedRef,
		UpdatedRef: in.UpdatedRef,
		DeletedRef: in.DeletedRef,
	}
}

func ToProto(in goku.KV) *KV {
	return &KV{
		Key:        in.Key,
		Value:      in.Value,
		Version:    in.Version,
		CreatedRef: in.CreatedRef,
		UpdatedRef: in.UpdatedRef,
		DeletedRef: in.DeletedRef,
	}
}
