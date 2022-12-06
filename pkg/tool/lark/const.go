/*
 * Copyright 2022 The KodeRover Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lark

type ApprovalType string

const (
	ApproveTypeAnd ApprovalType = "AND"
	ApproveTypeOr  ApprovalType = "OR"
)

const (
	// ApproverSelectionMethodFree is the approver selection method in the definition of approval
	// Free means the approval sponsor can choose the approver freely
	ApproverSelectionMethodFree = "Free"

	approvalNameI18NKey        = `@i18n@approval_name`
	approvalDescriptionI18NKey = `@i18n@description`
	approvalNodeApproveI18NKey = `@i18n@node_approve`
	approvalFormNameI18NKey    = `@i18n@formname`
	approvalFormValueI18NKey   = `@i18n@formvalue`

	approvalFormNameI18NValue = `详情`

	defaultFormValueI18NValue = `用于 Zadig Workflow 审批`
	defaultNodeApproveValue   = `审批`

	defaultPageSize = 50
)
