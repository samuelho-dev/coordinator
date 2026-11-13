package wire

import "encoding/json"

type presenceJSON SessionPresence

func (p *SessionPresence) UnmarshalJSON(b []byte) error {
	var v presenceJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*p = SessionPresence(v)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	for _, k := range []string{"nativeUpdatedAt", "startedAt", "lastActivityAt"} {
		if v, ok := raw[k]; ok && string(v) == "null" {
			if p.nullable == nil {
				p.nullable = map[string]bool{}
			}
			p.nullable[k] = true
		}
	}
	return nil
}
func (p SessionPresence) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(presenceJSON(p))
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for k := range p.nullable {
		if _, ok := m[k]; !ok {
			m[k] = json.RawMessage("null")
		}
	}
	return json.Marshal(m)
}

type listSessionJSON ListSession

func (s ListSession) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(listSessionJSON(s))
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for k := range s.nullable {
		if k == "startedAt" || k == "lastActivityAt" {
			if _, ok := m[k]; !ok {
				m[k] = json.RawMessage("null")
			}
		}
	}
	return json.Marshal(m)
}
func (s *ListSession) UnmarshalJSON(b []byte) error {
	var v listSessionJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*s = ListSession(v)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	for _, k := range []string{"startedAt", "lastActivityAt"} {
		if v, ok := raw[k]; ok && string(v) == "null" {
			if s.nullable == nil {
				s.nullable = map[string]bool{}
			}
			s.nullable[k] = true
		}
	}
	return nil
}
