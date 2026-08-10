package mds

import (
	"context"

	"google.golang.org/grpc"
)

// serviceDesc is what protoc-gen-go-grpc would have generated. It is
// written out by hand here for the reason given in codec.go: no protoc in
// this environment. The shape is exactly the generated shape — grpc-go
// does not care where a ServiceDesc came from — so the handlers below are
// mechanical, and deliberately contain no logic beyond decode/dispatch.

// MetadataServer is the service interface. grpc.ServiceDesc.HandlerType
// must be a pointer to an *interface* — grpc-go reflects on it to check
// the registered implementation satisfies the service — so this exists
// for the same reason the generated `FooServer` interface does, not as
// an abstraction anything in this repo needs for its own sake.
type MetadataServer interface {
	GetInode(context.Context, *GetInodeRequest) (*GetInodeResponse, error)
	Lookup(context.Context, *LookupRequest) (*LookupResponse, error)
	Readdir(context.Context, *ReaddirRequest) (*ReaddirResponse, error)
	Commit(context.Context, *CommitRequest) (*CommitResponse, error)
	Unlink(context.Context, *UnlinkRequest) (*UnlinkResponse, error)
	AckRecall(context.Context, *AckRecallRequest) (*AckRecallResponse, error)
	GetLocator(context.Context, *GetLocatorRequest) (*GetLocatorResponse, error)
	PutLocator(context.Context, *PutLocatorRequest) (*PutLocatorResponse, error)
	HasLocator(context.Context, *HasLocatorRequest) (*HasLocatorResponse, error)
	Mkdir(context.Context, *MkdirRequest) (*MkdirResponse, error)
	Rmdir(context.Context, *RmdirRequest) (*RmdirResponse, error)
	Rename(context.Context, *RenameRequest) (*RenameResponse, error)
	Symlink(context.Context, *SymlinkRequest) (*SymlinkResponse, error)
	Subscribe(*SubscribeRequest, grpc.ServerStream) error
}

var _ MetadataServer = (*Server)(nil)

func unaryHandler[Req any, Resp any](
	method func(MetadataServer, context.Context, *Req) (*Resp, error),
	fullMethod string,
) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := new(Req)
		if err := dec(in); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return method(srv.(MetadataServer), ctx, in)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: fullMethod}
		return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
			return method(srv.(MetadataServer), ctx, req.(*Req))
		})
	}
}

func subscribeHandler(srv any, stream grpc.ServerStream) error {
	req := new(SubscribeRequest)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	return srv.(MetadataServer).Subscribe(req, stream)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*MetadataServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "GetInode", Handler: unaryHandler(MetadataServer.GetInode, MethodGetInode)},
		{MethodName: "Lookup", Handler: unaryHandler(MetadataServer.Lookup, MethodLookup)},
		{MethodName: "Readdir", Handler: unaryHandler(MetadataServer.Readdir, MethodReaddir)},
		{MethodName: "Commit", Handler: unaryHandler(MetadataServer.Commit, MethodCommit)},
		{MethodName: "Unlink", Handler: unaryHandler(MetadataServer.Unlink, MethodUnlink)},
		{MethodName: "AckRecall", Handler: unaryHandler(MetadataServer.AckRecall, MethodAckRecall)},
		{MethodName: "GetLocator", Handler: unaryHandler(MetadataServer.GetLocator, MethodGetLocator)},
		{MethodName: "PutLocator", Handler: unaryHandler(MetadataServer.PutLocator, MethodPutLocator)},
		{MethodName: "HasLocator", Handler: unaryHandler(MetadataServer.HasLocator, MethodHasLocator)},
		{MethodName: "Mkdir", Handler: unaryHandler(MetadataServer.Mkdir, MethodMkdir)},
		{MethodName: "Rmdir", Handler: unaryHandler(MetadataServer.Rmdir, MethodRmdir)},
		{MethodName: "Rename", Handler: unaryHandler(MetadataServer.Rename, MethodRename)},
		{MethodName: "Symlink", Handler: unaryHandler(MetadataServer.Symlink, MethodSymlink)},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Subscribe", Handler: subscribeHandler, ServerStreams: true},
	},
	Metadata: "atlas/mds/v1",
}
