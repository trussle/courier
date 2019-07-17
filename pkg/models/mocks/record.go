// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/trussle/courier/pkg/models (interfaces: Record)

// Package mocks is a generated GoMock package.
package mocks

import (
	gomock "github.com/golang/mock/gomock"
	models "github.com/trussle/courier/pkg/models"
	uuid "github.com/trussle/uuid"
	reflect "reflect"
)

// MockRecord is a mock of Record interface
type MockRecord struct {
	ctrl     *gomock.Controller
	recorder *MockRecordMockRecorder
}

// MockRecordMockRecorder is the mock recorder for MockRecord
type MockRecordMockRecorder struct {
	mock *MockRecord
}

// NewMockRecord creates a new mock instance
func NewMockRecord(ctrl *gomock.Controller) *MockRecord {
	mock := &MockRecord{ctrl: ctrl}
	mock.recorder = &MockRecordMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRecord) EXPECT() *MockRecordMockRecorder {
	return m.recorder
}

// Body mocks base method
func (m *MockRecord) Body() []byte {
	ret := m.ctrl.Call(m, "Body")
	ret0, _ := ret[0].([]byte)
	return ret0
}

// Body indicates an expected call of Body
func (mr *MockRecordMockRecorder) Body() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Body", reflect.TypeOf((*MockRecord)(nil).Body))
}

// Commit mocks base method
func (m *MockRecord) Commit(arg0 models.Transaction) error {
	ret := m.ctrl.Call(m, "Commit", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit
func (mr *MockRecordMockRecorder) Commit(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockRecord)(nil).Commit), arg0)
}

// Equal mocks base method
func (m *MockRecord) Equal(arg0 models.Record) bool {
	ret := m.ctrl.Call(m, "Equal", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Equal indicates an expected call of Equal
func (mr *MockRecordMockRecorder) Equal(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Equal", reflect.TypeOf((*MockRecord)(nil).Equal), arg0)
}

// Failed mocks base method
func (m *MockRecord) Failed(arg0 models.Transaction) error {
	ret := m.ctrl.Call(m, "Failed", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Failed indicates an expected call of Failed
func (mr *MockRecordMockRecorder) Failed(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Failed", reflect.TypeOf((*MockRecord)(nil).Failed), arg0)
}

// ID mocks base method
func (m *MockRecord) ID() uuid.UUID {
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(uuid.UUID)
	return ret0
}

// ID indicates an expected call of ID
func (mr *MockRecordMockRecorder) ID() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockRecord)(nil).ID))
}

// Receipt mocks base method
func (m *MockRecord) Receipt() models.Receipt {
	ret := m.ctrl.Call(m, "Receipt")
	ret0, _ := ret[0].(models.Receipt)
	return ret0
}

// Receipt indicates an expected call of Receipt
func (mr *MockRecordMockRecorder) Receipt() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Receipt", reflect.TypeOf((*MockRecord)(nil).Receipt))
}

// RecordID mocks base method
func (m *MockRecord) RecordID() string {
	ret := m.ctrl.Call(m, "RecordID")
	ret0, _ := ret[0].(string)
	return ret0
}

// RecordID indicates an expected call of RecordID
func (mr *MockRecordMockRecorder) RecordID() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordID", reflect.TypeOf((*MockRecord)(nil).RecordID))
}