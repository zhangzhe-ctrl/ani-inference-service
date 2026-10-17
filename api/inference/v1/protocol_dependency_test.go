package inferencev1

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestRetiredGPUAttachmentsCannotEnterBusinessPayload(t *testing.T) {
	for _, tc := range []struct {
		message proto.Message
		number  protoreflect.FieldNumber
	}{
		{message: &CreateInferenceServiceRequest{}, number: 10},
		{message: &DeleteInferenceServiceRequest{}, number: 4},
	} {
		descriptor := tc.message.ProtoReflect().Descriptor()
		if descriptor.Fields().ByName("gpu_owner_attachment") != nil ||
			!descriptor.ReservedNames().Has("gpu_owner_attachment") ||
			!descriptor.ReservedRanges().Has(tc.number) {
			t.Fatal("removed attachment is not reserved", descriptor.FullName())
		}
		if err := protojson.Unmarshal([]byte(`{"gpu_owner_attachment":{}}`), tc.message); err == nil {
			t.Fatal("retired JSON attachment accepted", descriptor.FullName())
		}
	}
	request := &CreateInferenceServiceRequest{}
	retired := protowire.AppendTag(nil, 10, protowire.BytesType)
	retired = protowire.AppendBytes(retired, nil)
	if err := proto.Unmarshal(retired, request); err != nil {
		t.Fatal(err)
	}
	if _, err := CanonicalBusinessPayload(request); err == nil {
		t.Fatal("retired wire attachment entered canonical business payload")
	}
}

func TestDurableAckUsesPublicCommandProtocol(t *testing.T) {
	field := (&OperationResponse{}).ProtoReflect().Descriptor().Fields().ByName("durable_owner_ack")
	if field == nil || field.Message().FullName() != "accelerator.command.v1.DurableOwnerAck" {
		t.Fatal("durable ACK depends on retired owner attachment protocol", field)
	}
}
