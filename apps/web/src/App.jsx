import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Play, Database, Activity, Users, AlertTriangle } from 'lucide-react';

function App() {
  const [customers, setCustomers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(false);

  const fetchCustomers = async () => {
    try {
      const res = await axios.get('http://localhost:8000/api/customers');
      setCustomers(res.data.data || []);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCustomers();
    const interval = setInterval(fetchCustomers, 5000);
    return () => clearInterval(interval);
  }, []);

  const runPipeline = async () => {
    setTriggering(true);
    try {
      await axios.post('http://localhost:8000/api/jobs/run');
      setTimeout(() => {
        fetchCustomers();
        setTriggering(false);
      }, 3000);
    } catch (e) {
      console.error(e);
      setTriggering(false);
    }
  };

  return (
    <div className="min-h-screen bg-background p-8">
      <header className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
            ETL Control Center
          </h1>
          <p className="text-slate-400 mt-1">Monitor and manage your data pipelines</p>
        </div>
        <button
          onClick={runPipeline}
          disabled={triggering}
          className="flex items-center gap-2 bg-primary hover:bg-blue-600 transition-colors px-6 py-3 rounded-lg font-medium shadow-lg shadow-primary/20 disabled:opacity-50"
        >
          <Play size={20} className={triggering ? 'animate-pulse' : ''} />
          {triggering ? 'Pipeline Running...' : 'Trigger Pipeline'}
        </button>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-surface p-6 rounded-xl border border-slate-700/50 flex items-center gap-4">
          <div className="p-3 bg-blue-500/10 rounded-lg text-blue-400">
            <Database size={24} />
          </div>
          <div>
            <p className="text-slate-400 text-sm">Total Records</p>
            <p className="text-2xl font-bold">{customers.length}</p>
          </div>
        </div>
        <div className="bg-surface p-6 rounded-xl border border-slate-700/50 flex items-center gap-4">
          <div className="p-3 bg-red-500/10 rounded-lg text-red-400">
            <AlertTriangle size={24} />
          </div>
          <div>
            <p className="text-slate-400 text-sm">High Risk Churn</p>
            <p className="text-2xl font-bold">{customers.filter(c => c.churn_risk_score > 50).length}</p>
          </div>
        </div>
        <div className="bg-surface p-6 rounded-xl border border-slate-700/50 flex items-center gap-4">
          <div className="p-3 bg-green-500/10 rounded-lg text-green-400">
            <Activity size={24} />
          </div>
          <div>
            <p className="text-slate-400 text-sm">System Status</p>
            <p className="text-2xl font-bold text-green-400">Healthy</p>
          </div>
        </div>
      </div>

      <div className="bg-surface rounded-xl border border-slate-700/50 overflow-hidden">
        <div className="p-6 border-b border-slate-700/50 flex justify-between items-center">
          <h2 className="text-xl font-semibold flex items-center gap-2">
            <Users size={20} className="text-accent"/> Customer Metrics
          </h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead className="bg-slate-800/50 text-slate-400 text-sm uppercase tracking-wider">
              <tr>
                <th className="p-4">Customer ID</th>
                <th className="p-4">Country</th>
                <th className="p-4">Plan</th>
                <th className="p-4">Activity Band</th>
                <th className="p-4">Churn Risk</th>
                <th className="p-4">Last Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {customers.map((c, i) => (
                <tr key={i} className="hover:bg-slate-800/20 transition-colors">
                  <td className="p-4 font-mono text-sm">{c.customer_id}</td>
                  <td className="p-4">{c.country}</td>
                  <td className="p-4">
                    <span className="px-2 py-1 rounded-full text-xs font-medium bg-slate-700 text-slate-300">
                      {c.plan}
                    </span>
                  </td>
                  <td className="p-4">
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${c.activity_band === 'high' ? 'bg-green-500/20 text-green-400' : c.activity_band === 'medium' ? 'bg-yellow-500/20 text-yellow-400' : 'bg-red-500/20 text-red-400'}`}>
                      {c.activity_band?.toUpperCase()}
                    </span>
                  </td>
                  <td className="p-4">
                    <div className="flex items-center gap-2">
                      <div className="w-24 h-2 bg-slate-700 rounded-full overflow-hidden">
                        <div 
                          className={`h-full ${c.churn_risk_score > 50 ? 'bg-red-500' : c.churn_risk_score > 20 ? 'bg-yellow-500' : 'bg-green-500'}`}
                          style={{ width: `${Math.min(c.churn_risk_score, 100)}%` }}
                        />
                      </div>
                      <span className="text-sm font-mono">{Math.round(c.churn_risk_score)}</span>
                    </div>
                  </td>
                  <td className="p-4 text-sm text-slate-400">
                    {new Date(c.updated_at).toLocaleString()}
                  </td>
                </tr>
              ))}
              {customers.length === 0 && !loading && (
                <tr>
                  <td colSpan={6} className="p-8 text-center text-slate-400">
                    No data found. Trigger the pipeline to extract data.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default App;
