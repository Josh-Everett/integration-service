/*
Copyright 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package component

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	h "github.com/konflux-ci/integration-service/helpers"
	tektonconsts "github.com/konflux-ci/integration-service/tekton/consts"
)

// ensureIntegrationPipelineServiceAccount makes sure the per-component integration pipeline
// ServiceAccount its-pipeline-<owner name> and the RoleBinding of the same name to the
// its-pipeline-runner ClusterRole exist in the owner's namespace and carry an owner reference to the
// owner (STONEINTG-1799). The owner is a Component of any kind. The reference is a plain owner
// reference, as build-service uses for build-pipeline-<component>: several Components may own one
// account while a tenant migrates between Component kinds, and garbage collection removes the account
// when the last owner goes. An object that already exists is adopted by adding the owner's reference;
// a reference left by a previous Component of the same name and kind is replaced. The push credential
// is linked to the account by image-controller (STONEINTG-1804).
func ensureIntegrationPipelineServiceAccount(ctx context.Context, c client.Client, logger h.IntegrationLogger, owner client.Object) error {
	name := h.ComponentIntegrationPipelineServiceAccountName(owner.GetName())
	namespace := owner.GetNamespace()

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := ensureOwnedObject(ctx, c, logger, owner, "ServiceAccount", sa); err != nil {
		return fmt.Errorf("failed to ensure ServiceAccount %s/%s: %w", namespace, name, err)
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     tektonconsts.ComponentIntegrationPipelineClusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
	}
	if err := ensureOwnedObject(ctx, c, logger, owner, "RoleBinding", rb); err != nil {
		return fmt.Errorf("failed to ensure RoleBinding %s/%s: %w", namespace, name, err)
	}

	return nil
}

// ensureOwnedObject creates obj with an owner reference to owner or, when an object of that name
// already exists, adopts it. kind is only used in log lines.
func ensureOwnedObject(ctx context.Context, c client.Client, logger h.IntegrationLogger, owner client.Object, kind string, obj client.Object) error {
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	existing := obj.DeepCopyObject().(client.Object)

	err := c.Get(ctx, key, existing)
	if err == nil {
		return adoptOwnedObject(ctx, c, logger, owner, kind, existing)
	}
	if !errors.IsNotFound(err) {
		return err
	}

	if err := controllerutil.SetOwnerReference(owner, obj, c.Scheme()); err != nil {
		return err
	}
	if err := c.Create(ctx, obj); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
		// Created concurrently or not yet in the cache: adopt it instead.
		if err := c.Get(ctx, key, existing); err != nil {
			return err
		}
		return adoptOwnedObject(ctx, c, logger, owner, kind, existing)
	}

	logger.Info("Created object for component", "kind", kind, "name", key.Name, "namespace", key.Namespace)
	return nil
}

// adoptOwnedObject adds owner's reference to existing when no reference with owner's UID is present.
// SetOwnerReference overwrites a reference that has the same API group, kind and name (a Component
// deleted and recreated under the same name) and appends next to any other owner.
func adoptOwnedObject(ctx context.Context, c client.Client, logger h.IntegrationLogger, owner client.Object, kind string, existing client.Object) error {
	for _, ref := range existing.GetOwnerReferences() {
		if ref.UID == owner.GetUID() {
			return nil
		}
	}

	if err := controllerutil.SetOwnerReference(owner, existing, c.Scheme()); err != nil {
		return err
	}
	if err := c.Update(ctx, existing); err != nil {
		return err
	}

	logger.Info("Adopted existing object for component", "kind", kind, "name", existing.GetName(), "namespace", existing.GetNamespace())
	return nil
}
