package perconaservermongodb

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	api "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/percona/percona-server-mongodb-operator/pkg/naming"
	"github.com/percona/percona-server-mongodb-operator/pkg/psmdb/config"
)

const annotationSelectedNode = "volume.kubernetes.io/selected-node"

// pvcMemberInfo holds information about a PVC, its AZ, and member status.
type pvcMemberInfo struct {
	pvcName   string
	zone      string
	ordinal   int32
	isPrimary bool
}

// preCreateClonedPVCs creates PVCs with dataSource for new replicas before
// scaling up the StatefulSet. Returns true if all needed PVCs are created and
// the StatefulSet can safely scale up.
func (r *ReconcilePerconaServerMongoDB) preCreateClonedPVCs(
	ctx context.Context,
	cr *api.PerconaServerMongoDB,
	rs *api.ReplsetSpec,
	sts *appsv1.StatefulSet,
	component string,
) (bool, error) {
	log := logf.FromContext(ctx).WithName("VolumeClone").WithValues("replset", rs.Name, "component", component)

	if sts.Spec.Replicas == nil {
		return true, nil
	}

	currentReplicas := *sts.Spec.Replicas
	desiredReplicas := componentDesiredSize(rs, component)
	stsName := sts.Name

	// If volumeClone is disabled, check for leftover pending clone PVCs.
	// If any exist, block scale-up and log guidance.
	if rs.VolumeClone == nil || !rs.VolumeClone.Enabled {
		if desiredReplicas > currentReplicas {
			if hasPending, pvcNames := r.hasPendingClonePVCs(ctx, cr.Namespace, stsName, currentReplicas, desiredReplicas); hasPending {
				log.Error(nil, "volumeClone is disabled but pending cloned PVCs exist, scale-up is blocked. "+
					"Restore the replica count to its previous value to clean up pending PVCs",
					"pendingPVCs", pvcNames)
				return false, nil
			}
		}
		return true, nil
	}

	if desiredReplicas <= currentReplicas {
		return true, nil
	}

	// Collect source candidates from all data-bearing components in the replica set.
	members, err := r.collectAllMemberPVCInfo(ctx, cr, rs)
	if err != nil {
		return false, errors.Wrap(err, "collect member PVC info")
	}

	if len(members) == 0 {
		log.Info("No existing members with bound PVCs found, skipping volume clone")
		return true, nil
	}

	for i := currentReplicas; i < desiredReplicas; i++ {
		pvcName := fmt.Sprintf("%s-%s-%d",
			config.MongodDataVolClaimName, stsName, i)

		existingPVC := &corev1.PersistentVolumeClaim{}
		err := r.client.Get(ctx, types.NamespacedName{
			Name:      pvcName,
			Namespace: cr.Namespace,
		}, existingPVC)

		if err == nil {
			log.Info("Cloned PVC already exists, skipping creation",
				"pvc", pvcName, "phase", existingPVC.Status.Phase)
			continue
		}

		if !k8serrors.IsNotFound(err) {
			return false, errors.Wrapf(err, "get PVC %s", pvcName)
		}

		source := selectSourceByAZ(members)
		if source == nil {
			log.Info("No suitable source PVC found for cloning, falling back to Initial Sync",
				"pvc", pvcName)
			continue
		}

		sourcePVC := &corev1.PersistentVolumeClaim{}
		if err := r.client.Get(ctx, types.NamespacedName{
			Name:      source.pvcName,
			Namespace: cr.Namespace,
		}, sourcePVC); err != nil {
			return false, errors.Wrapf(err, "get source PVC %s", source.pvcName)
		}

		newPVC := buildClonedPVC(pvcName, cr.Namespace, sourcePVC)
		if err := r.client.Create(ctx, newPVC); err != nil {
			return false, errors.Wrapf(err, "create cloned PVC %s", pvcName)
		}

		log.Info("Created cloned PVC",
			"pvc", pvcName, "source", source.pvcName, "zone", source.zone)

		// Track the new PVC's AZ for subsequent iterations
		members = append(members, pvcMemberInfo{
			pvcName: pvcName,
			zone:    source.zone,
			ordinal: i,
		})
	}

	return true, nil
}

// hasPendingClonePVCs checks if there are any pending PVCs created by volume clone
// in the ordinal range [from, to).
func (r *ReconcilePerconaServerMongoDB) hasPendingClonePVCs(
	ctx context.Context,
	namespace, stsName string,
	from, to int32,
) (bool, []string) {
	var pendingPVCs []string
	for i := from; i < to; i++ {
		pvcName := fmt.Sprintf("%s-%s-%d",
			config.MongodDataVolClaimName, stsName, i)

		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.client.Get(ctx, types.NamespacedName{
			Name:      pvcName,
			Namespace: namespace,
		}, pvc); err != nil {
			continue
		}

		if pvc.Status.Phase != corev1.ClaimBound {
			if _, ok := pvc.Annotations[api.AnnotationVolumeCloneSource]; ok {
				pendingPVCs = append(pendingPVCs, pvcName)
			}
		}
	}
	return len(pendingPVCs) > 0, pendingPVCs
}

// componentDesiredSize returns the desired replica count for the given component.
func componentDesiredSize(rs *api.ReplsetSpec, component string) int32 {
	switch component {
	case naming.ComponentNonVoting:
		return rs.NonVoting.GetSize()
	case naming.ComponentHidden:
		return rs.Hidden.GetSize()
	default:
		return rs.Size
	}
}

// componentStsSuffix returns the StatefulSet name suffix for the given component.
func componentStsSuffix(component string) string {
	switch component {
	case naming.ComponentNonVoting:
		return "-" + naming.ComponentNonVotingShort
	case naming.ComponentHidden:
		return "-" + naming.ComponentHidden
	default:
		return ""
	}
}

// collectAllMemberPVCInfo gathers PVC and AZ information for all data-bearing members
// across all components (mongod, nonVoting, hidden) in the replica set.
// AZ is determined from the PV's topology.kubernetes.io/zone label.
func (r *ReconcilePerconaServerMongoDB) collectAllMemberPVCInfo(
	ctx context.Context,
	cr *api.PerconaServerMongoDB,
	rs *api.ReplsetSpec,
) ([]pvcMemberInfo, error) {
	log := logf.FromContext(ctx).WithName("VolumeClone")

	// Get all mongod pods to determine primary status
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList,
		client.InNamespace(cr.Namespace),
		client.MatchingLabels(naming.MongodLabels(cr, rs)),
	); err != nil {
		return nil, errors.Wrap(err, "list pods")
	}

	primaryPod := ""
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		isPrimary, err := r.isPodPrimary(ctx, cr, pod, rs)
		if err != nil {
			continue
		}
		if isPrimary {
			primaryPod = pod.Name
			break
		}
	}

	// Collect PVCs from all data-bearing components.
	type componentInfo struct {
		component string
		size      int32
	}
	components := []componentInfo{
		{naming.ComponentMongod, rs.Size},
		{naming.ComponentNonVoting, rs.NonVoting.GetSize()},
		{naming.ComponentHidden, rs.Hidden.GetSize()},
	}

	var members []pvcMemberInfo
	for _, comp := range components {
		if comp.size == 0 {
			continue
		}

		stsName := cr.Name + "-" + rs.Name + componentStsSuffix(comp.component)

		for i := int32(0); i < comp.size; i++ {
			pvcName := fmt.Sprintf("%s-%s-%d",
				config.MongodDataVolClaimName, stsName, i)
			podName := fmt.Sprintf("%s-%d", stsName, i)

			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.client.Get(ctx, types.NamespacedName{
				Name:      pvcName,
				Namespace: cr.Namespace,
			}, pvc); err != nil {
				continue
			}

			if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
				continue
			}

			pv := &corev1.PersistentVolume{}
			if err := r.client.Get(ctx, types.NamespacedName{
				Name: pvc.Spec.VolumeName,
			}, pv); err != nil {
				log.Error(err, "Failed to get PV for PVC, skipping member for volume clone source selection",
					"pvc", pvcName, "pv", pvc.Spec.VolumeName)
				continue
			}

			zone := pv.Labels["topology.kubernetes.io/zone"]

			members = append(members, pvcMemberInfo{
				pvcName:   pvcName,
				zone:      zone,
				ordinal:   i,
				isPrimary: podName == primaryPod,
			})
		}
	}

	return members, nil
}

// selectSourceByAZ selects the best source PVC for cloning based on AZ
// distribution. It picks from the AZ with the fewest members to maintain
// balanced distribution, preferring secondary members over the primary.
func selectSourceByAZ(members []pvcMemberInfo) *pvcMemberInfo {
	if len(members) == 0 {
		return nil
	}

	// Count members per AZ
	azCount := map[string]int{}
	for _, m := range members {
		azCount[m.zone]++
	}

	// Find the minimum count
	minCount := -1
	for _, count := range azCount {
		if minCount < 0 || count < minCount {
			minCount = count
		}
	}

	// Collect candidate AZs (those with minimum count)
	candidateAZs := map[string]bool{}
	for az, count := range azCount {
		if count == minCount {
			candidateAZs[az] = true
		}
	}

	// Filter members in candidate AZs, sorted: secondaries first, then by ordinal
	candidates := make([]pvcMemberInfo, 0)
	for _, m := range members {
		if candidateAZs[m.zone] {
			candidates = append(candidates, m)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		// Secondary before primary
		if candidates[i].isPrimary != candidates[j].isPrimary {
			return !candidates[i].isPrimary
		}
		// Lower ordinal first
		return candidates[i].ordinal < candidates[j].ordinal
	})

	return &candidates[0]
}

// buildClonedPVC creates a PVC spec that clones from the source PVC.
// It copies the selected-node annotation from the source to ensure the clone
// is provisioned in the same AZ as the source volume.
func buildClonedPVC(name, namespace string, source *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	annotations := map[string]string{
		api.AnnotationVolumeCloneSource: source.Name,
	}

	// Copy the selected-node annotation from the source PVC to ensure the
	// clone volume is provisioned in the same AZ. Without this, the scheduler
	// may place the Pod in a different AZ, causing cross-AZ clone failure.
	if selectedNode, ok := source.Annotations[annotationSelectedNode]; ok {
		annotations[annotationSelectedNode] = selectedNode
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      source.Labels,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      source.Spec.AccessModes,
			StorageClassName: source.Spec.StorageClassName,
			Resources:        source.Spec.Resources,
			DataSource: &corev1.TypedLocalObjectReference{
				Kind: "PersistentVolumeClaim",
				Name: source.Name,
			},
		},
	}
}
