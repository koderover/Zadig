package updater

import (
	"context"
	"fmt"
	"github.com/openkruise/kruise-api/apps/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ScaleCloneSet(ns, name string, replicas int, cl client.Client) error {
	patchBytes := []byte(fmt.Sprintf(`{"spec":{"replicas": %d}}`, replicas))
	return PatchCloneSet(ns, name, patchBytes, cl)
}

func PatchCloneSet(ns, name string, patchBytes []byte, cl client.Client) error {
	return cl.Patch(context.TODO(), &v1alpha1.CloneSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
	}, client.RawPatch(types.MergePatchType, patchBytes))
}
