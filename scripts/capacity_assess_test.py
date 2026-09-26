import unittest
from capacity_assess import assess

class AssessmentTests(unittest.TestCase):
    def report(self, seconds=360, terminal_rate=None):
        rate=100
        rows=[]
        for t in range(seconds+1):
            terminal=1200*max(0,(t-24)//12) if terminal_rate is None else terminal_rate*t
            rows.append({'seconds':t,'phase':'load','chains':{'x':{'admitted':rate*t,'attempted':rate*t,
                'included':1200*(t//12),'terminal':terminal}}})
        return {'samples':rows,'offered_phase_end_seconds':seconds,'warmup_seconds':60,
            'outcome':'pass','chaos':'none','oracle':{'safety_pass':True,'liveness_pass':True},
            'per_chain':{'x':{'responses':{'202':rate*seconds},'offered':rate*seconds,
                'scheduled_to_response_latency_ms':{'p99':50},'late_window':{'capacity_candidate':True}}}}
    def test_aligned_12_second_sawtooth_is_steady(self):
        result=assess(self.report(),'x',{'rate':100,'block_seconds':12})
        self.assertEqual(result['classification'],'steady_window_evidence_requires_repetition')
    def test_3_percent_terminal_growth_is_never_promoted(self):
        result=assess(self.report(terminal_rate=97),'x',{'rate':100,'block_seconds':12})
        self.assertEqual(result['classification'],'observed_persistent_backlog_growth')
        self.assertAlmostEqual(result['aligned_mean_slopes_tps']['terminal'],3)
    def test_short_clean_screen_stays_unconfirmed(self):
        result=assess(self.report(seconds=180),'x',{'rate':100,'block_seconds':12})
        self.assertIn('confirmation_window_too_short',result['reasons'])
        self.assertNotEqual(result['classification'],'steady_window_evidence_requires_repetition')
    def test_tiny_sample_clock_drift_does_not_discard_whole_window(self):
        report=self.report(seconds=180)
        report['samples'][-1]['seconds']-=.001
        result=assess(report,'x',{'rate':100,'block_seconds':12})
        self.assertGreater(result['window_seconds'],100)

if __name__=='__main__':unittest.main()
