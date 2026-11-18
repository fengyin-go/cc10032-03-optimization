import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import SinkRecoveryView from '@/views/SinkRecoveryView.vue'
import SinkReportDetailView from '@/views/SinkReportDetailView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径需排在动态模块路由之前，避免被 /:moduleKey 抢占
    { path: '/sink-recovery', name: 'sink-recovery', component: SinkRecoveryView },
    {
      path: '/sink-recovery/report/:publicationId',
      name: 'sink-report-detail',
      component: SinkReportDetailView,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
