"""Disk pressure traces: synthetic statvfs observations, no filesystem filling or RPC."""
import unittest

import capacity_resource_guard as guard


def sample(stamp, available):
    return {'monotonic': stamp, 'resources': {
        'host_bytes': guard.Capacity('host', 228 * guard.GIB, int(available))}}


def policy(interval=60, declared=2 * guard.MIB, horizon=3000):
    return guard.ResourcePolicy({'host_bytes': guard.Budget(8 * guard.GIB, declared, .02)},
                                horizon, interval_seconds=interval)


class SustainedProjection(unittest.TestCase):
    def test_one_minute_burst_does_not_become_permanent_full_horizon_rate(self):
        p = policy(); initial = 87 * guard.GIB; consumed = 24 * guard.MIB * 60
        self.assertTrue(p.assess(sample(0, initial))['allowed'])
        for t in (60, 120, 180, 240, 300):
            result = p.assess(sample(t, initial - consumed))
            self.assertTrue(result['allowed'], (t, result))
            fields = result['resources']['host_bytes']
            self.assertEqual(fields['peak_observed_consumption_per_second'], 24 * guard.MIB)
            if t == 60:
                self.assertFalse(fields['sustained_observation_ready'])
                self.assertEqual(fields['projected_growth_per_second'], 4 * guard.MIB)
                self.assertEqual(fields['emergency_growth_per_second'], 48 * guard.MIB)
            if t == 180:
                self.assertTrue(fields['sustained_observation_ready'])
                self.assertEqual(fields['sustained_observed_consumption_per_second'], 8 * guard.MIB)
            if t >= 240:
                self.assertEqual(fields['recent_peak_consumption_per_second'], 0)
                self.assertEqual(fields['projected_growth_per_second'], 4 * guard.MIB)

    def test_sustained_underdeclared_consumption_stops_at_three_minutes(self):
        p = policy(); initial = 87 * guard.GIB; rate = 24 * guard.MIB
        for t in (0, 60, 120):
            self.assertTrue(p.assess(sample(t, initial - rate * t))['allowed'])
        stopped = p.assess(sample(180, initial - rate * 180))
        self.assertFalse(stopped['allowed'])
        fields = stopped['resources']['host_bytes']
        self.assertTrue(fields['sustained_observation_ready'])
        self.assertEqual(fields['sustained_observed_consumption_per_second'], rate)
        self.assertGreater(fields['available'] - fields['floor'],
                           fields['emergency_growth_per_second'] * fields['emergency_reserve_seconds'])
        # Recovery of free space, a smaller finite phase, and aging-out of samples
        # must not turn a stopped campaign back into an allowed campaign.
        p.begin_reconciliation(181, 60)
        recovered = p.assess(sample(200, initial))
        self.assertFalse(recovered['allowed'])
        self.assertEqual(recovered['stop'], stopped['stop'])

    def test_first_observed_fast_decline_stops_before_the_floor_is_reached(self):
        p = policy(); initial = 87 * guard.GIB
        p.assess(sample(0, initial))
        stopped = p.assess(sample(60, initial - 300 * guard.MIB * 60))
        self.assertFalse(stopped['allowed'])
        fields = stopped['resources']['host_bytes']
        self.assertFalse(fields['sustained_observation_ready'])
        self.assertGreater(fields['available'], fields['floor'])
        self.assertGreater(fields['emergency_growth_per_second'] * fields['emergency_reserve_seconds'],
                           fields['available'] - fields['floor'])

    def test_declared_growth_always_covers_full_horizon_without_observations(self):
        p = policy(declared=24 * guard.MIB)
        stopped = p.assess(sample(0, 87 * guard.GIB))
        self.assertFalse(stopped['allowed'])
        fields = stopped['resources']['host_bytes']
        self.assertEqual(fields['required_available'], 8 * guard.GIB + 48 * guard.MIB * (3000 + 180))
        self.assertFalse(fields['sustained_observation_ready'])

    def test_thirty_second_observations_still_require_three_minutes(self):
        p = policy(interval=30); initial = 87 * guard.GIB; rate = 24 * guard.MIB
        for t in range(0, 180, 30):
            result = p.assess(sample(t, initial - rate * t))
            self.assertTrue(result['allowed'])
            self.assertFalse(result['resources']['host_bytes']['sustained_observation_ready'])
        stopped = p.assess(sample(180, initial - rate * 180))
        self.assertFalse(stopped['allowed'])
        self.assertEqual(stopped['resources']['host_bytes']['observed_window_intervals'], 6)

    def test_one_late_observation_cannot_fake_three_intervals(self):
        p = policy(); initial = 87 * guard.GIB; rate = 24 * guard.MIB
        p.assess(sample(0, initial))
        one = p.assess(sample(180, initial - rate * 180))
        self.assertTrue(one['allowed'])
        self.assertFalse(one['resources']['host_bytes']['sustained_observation_ready'])
        p.assess(sample(240, initial - rate * 240))
        stopped = p.assess(sample(300, initial - rate * 300))
        self.assertFalse(stopped['allowed'])
        self.assertTrue(stopped['resources']['host_bytes']['sustained_observation_ready'])

    def test_short_cleanup_does_not_cancel_positive_consumption_in_window(self):
        p = policy(); initial = 87 * guard.GIB; consumed = 24 * guard.MIB * 60
        p.assess(sample(0, initial)); p.assess(sample(60, initial - consumed))
        p.assess(sample(120, initial))
        final = p.assess(sample(180, initial - consumed))['resources']['host_bytes']
        self.assertEqual(final['sustained_observed_consumption_per_second'], 16 * guard.MIB)
        self.assertEqual(final['recent_peak_consumption_per_second'], 24 * guard.MIB)

    def test_window_clips_partial_intervals_instead_of_averaging_samples(self):
        p = policy(); initial = 87 * guard.GIB
        p.assess(sample(0, initial))
        p.assess(sample(70, initial - 24 * guard.MIB * 70))
        p.assess(sample(130, initial - 24 * guard.MIB * 70))
        final = p.assess(sample(190, initial - 24 * guard.MIB * 70))['resources']['host_bytes']
        self.assertEqual(final['observed_window_seconds'], 180)
        self.assertTrue(final['sustained_observation_ready'])
        self.assertEqual(final['sustained_observed_consumption_per_second'], 8 * guard.MIB)

    def test_host_guest_and_inode_pressure_remain_independent(self):
        budgets = {'host_bytes': guard.Budget(8 * guard.GIB, 2 * guard.MIB, 0),
                   'guest_bytes': guard.Budget(4 * guard.GIB, guard.MIB, 0),
                   'guest_inodes': guard.Budget(100000, 4, 0)}
        p = guard.ResourcePolicy(budgets, 3000)
        def resources(t, guest):
            return {'monotonic': t, 'resources': {
                'host_bytes': guard.Capacity('host', 228 * guard.GIB, 87 * guard.GIB),
                'guest_bytes': guard.Capacity('guest', 24 * guard.GIB, int(guest)),
                'guest_inodes': guard.Capacity('guest', 1000000, 800000)}}
        self.assertTrue(p.assess(resources(0, 20 * guard.GIB))['allowed'])
        result = p.assess(resources(60, 16 * guard.GIB))
        self.assertFalse(result['allowed'])
        self.assertEqual(result['stop']['details'], ['guest_bytes:insufficient_projected_reserve'])
        self.assertEqual(result['resources']['host_bytes']['recent_peak_consumption_per_second'], 0)
        self.assertEqual(result['resources']['guest_inodes']['recent_peak_consumption_per_second'], 0)


if __name__ == '__main__': unittest.main()
