package inferencev1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CanonicalBusinessPayload is the public Gov/Inference business digest
// contract. It uses all snake_case business fields, absent messages as null,
// empty lists/maps, decimal strings for integers/enums, lexical object keys,
// UTF-8 without HTML escaping or a trailing newline. Command IDs and trusted
// attachments/charges are excluded. SHA256 has no additional prefix.
func CanonicalBusinessPayload(req *CreateInferenceServiceRequest) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("business request is required")
	}
	object, err := canonicalMessage(req.ProtoReflect(), true)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(object); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), nil
}

func BusinessPayloadDigest(req *CreateInferenceServiceRequest) (string, error) {
	raw, err := CanonicalBusinessPayload(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ManagedCreateRequestHash verifies the Governance original acceptance digest
// independently of the owner attachment. The action is the registered real
// Inference RPC, and the GPU shape is part of both business and admission.
func ManagedCreateRequestHash(req *CreateInferenceServiceRequest, tenantID, actorType, actorID string) (string, error) {
	if req == nil || req.GetResource().GetGpu() == nil {
		return "", fmt.Errorf("GPU business request is required")
	}
	business, err := canonicalMessage(req.ProtoReflect(), true)
	if err != nil {
		return "", err
	}
	selection, err := canonicalMessage(req.GetResource().GetGpu().ProtoReflect(), false)
	if err != nil {
		return "", err
	}
	return governanceHash(map[string]any{"schema": "gov-gpu-create-v1", "tenant_id": tenantID, "actor_type": actorType, "actor_id": actorID, "owner": "ani-inference", "action": "/inference.v1.InferenceServiceManager/CreateInferenceService", "gpu_request": selection, "business_type": "inference.v1.CreateInferenceServiceRequest", "business": business})
}

func ManagedDeleteRequestHash(tenantID, resourceID, originalCreateID, actorType, actorID string) (string, error) {
	return governanceHash(map[string]any{"schema": "gov-gpu-delete-v1", "tenant_id": tenantID, "actor_type": actorType, "actor_id": actorID, "owner": "ani-inference", "action": "/inference.v1.InferenceServiceManager/DeleteInferenceService", "resource_id": resourceID, "create_operation_id": originalCreateID})
}

func governanceHash(object map[string]any) (string, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(object); err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("acc-c14n-v1\n"), bytes.TrimSuffix(out.Bytes(), []byte{'\n'})...))
	return hex.EncodeToString(sum[:]), nil
}

func canonicalMessage(msg protoreflect.Message, business bool) (map[string]any, error) {
	if len(msg.GetUnknown()) != 0 {
		return nil, fmt.Errorf("unknown business fields are not allowed")
	}
	out := make(map[string]any)
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := string(field.Name())
		if business && (name == "request_id" || name == "gpu_owner_attachment" || name == "original_charges") {
			continue
		}
		value := msg.Get(field)
		if field.IsMap() {
			entries := make(map[string]any)
			var failure error
			value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
				converted, err := canonicalValue(field.MapValue(), item)
				if err != nil {
					failure = err
					return false
				}
				entries[key.String()] = converted
				return true
			})
			if failure != nil {
				return nil, failure
			}
			out[name] = entries
		} else if field.IsList() {
			entries := make([]any, 0, value.List().Len())
			for j := 0; j < value.List().Len(); j++ {
				converted, err := canonicalValue(field, value.List().Get(j))
				if err != nil {
					return nil, err
				}
				entries = append(entries, converted)
			}
			out[name] = entries
		} else if field.Kind() == protoreflect.MessageKind && !msg.Has(field) {
			out[name] = nil
		} else {
			converted, err := canonicalValue(field, value)
			if err != nil {
				return nil, err
			}
			out[name] = converted
		}
	}
	return out, nil
}

func canonicalValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (any, error) {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return canonicalMessage(value.Message(), false)
	case protoreflect.StringKind:
		return value.String(), nil
	case protoreflect.BoolKind:
		return value.Bool(), nil
	case protoreflect.EnumKind:
		if value.Enum() < 0 {
			return nil, fmt.Errorf("negative enum is outside the canonical contract")
		}
		return strconv.FormatInt(int64(value.Enum()), 10), nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		if value.Int() < 0 {
			return nil, fmt.Errorf("negative integer is outside the canonical contract")
		}
		return strconv.FormatInt(value.Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		if value.Uint() > math.MaxInt64 {
			return nil, fmt.Errorf("canonical integer overflows int64")
		}
		return strconv.FormatUint(value.Uint(), 10), nil
	default:
		return nil, fmt.Errorf("unsupported business field %s", field.FullName())
	}
}

// DecodeBusinessPayload reconstructs the persisted canonical body. A strict
// roundtrip check rejects unknown fields, noncanonical quantities, and trusted
// attachment fields; adapters then add those fields from the original ledger.
func DecodeBusinessPayload(raw []byte) (*CreateInferenceServiceRequest, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	req := new(CreateInferenceServiceRequest)
	if err := decodeEnums(object, req.ProtoReflect().Descriptor()); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(wire, req); err != nil {
		return nil, err
	}
	canonical, err := CanonicalBusinessPayload(req)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, raw) {
		return nil, fmt.Errorf("noncanonical business payload")
	}
	return req, nil
}

func decodeEnums(object map[string]any, descriptor protoreflect.MessageDescriptor) error {
	for key, value := range object {
		field := descriptor.Fields().ByName(protoreflect.Name(key))
		if field == nil {
			return fmt.Errorf("unknown business field %q", key)
		}
		if field.Kind() == protoreflect.EnumKind {
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("enum %s requires a decimal string", key)
			}
			number, err := strconv.ParseInt(text, 10, 32)
			if err != nil {
				return err
			}
			object[key] = json.Number(strconv.FormatInt(number, 10))
		} else if field.Kind() == protoreflect.MessageKind && !field.IsMap() && value != nil {
			nested, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("message %s is malformed", key)
			}
			if err := decodeEnums(nested, field.Message()); err != nil {
				return err
			}
		}
	}
	return nil
}
