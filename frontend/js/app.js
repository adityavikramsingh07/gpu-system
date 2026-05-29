// frontend/js/app.js
import { initThreeJS } from './three_viz.js';

// Module scripts are deferred by default, DOM is already ready.
const navItems = document.querySelectorAll('.nav-item');
const pages = document.querySelectorAll('.page');

function switchPage(pageId) {
    pages.forEach(p => p.classList.remove('active'));
    navItems.forEach(n => n.classList.remove('active'));
    
    document.getElementById(`page-${pageId}`).classList.add('active');
    document.querySelector(`.nav-item[data-page="${pageId}"]`).classList.add('active');
}

navItems.forEach(item => {
    item.addEventListener('click', () => {
        switchPage(item.dataset.page);
    });
});

// 2. Init 3D Visualizer on Overview page
initThreeJS('bg-canvas');

// 3. Header Metrics Mock
setInterval(() => {
    const elSess = document.getElementById('m-sess');
    const elTick = document.getElementById('m-tick');
    const elOps = document.getElementById('m-ops');
    if(elSess) elSess.textContent = 2 + Math.floor(Math.random() * 5);
    if(elTick) elTick.textContent = 4 + Math.floor(Math.random() * 10);
    if(elOps) elOps.textContent = 95 + Math.floor(Math.random() * 150);
}, 4500);

// 4. Telemetry Event Feed logic
const EVENTS = [
    {t:'sys', m:'COG planning_node: Gemini decomposed query', src:'COG'},
    {t:'sys', m:'DTB: XADD×11 to Redis streams | fan-out dispatched', src:'DTB'},
    {t:'warn',m:'DSW geological_expert: GSI survey data partial', src:'DSW-1'},
    {t:'sys', m:'SVB: ephemeral token issued | ttl=300s', src:'SVB'}
];
let evIdx = 0; 
const feedEl = document.getElementById('feed-rows');

function addEvent(msg, type, src) {
    if(!feedEl) return;
    const now = new Date(), time = now.toTimeString().slice(0,8);
    const bMap = {sys:'b-sys', warn:'b-warn', heal:'b-heal', err:'b-err', info:'b-info'};
    const lMap = {sys:'SYS', warn:'WARN', heal:'HEAL', err:'ERR', info:'COG'};
    
    const row = document.createElement('div'); 
    row.className = 'frow';
    row.innerHTML = `<span class="ftime">${time}</span>
        <span class="fbadge ${bMap[type]}">${lMap[type]}</span>
        <span class="fmsg">${msg}</span>
        <span class="fsrc">${src||''}</span>`;
        
    feedEl.appendChild(row);
    while(feedEl.children.length > 50) feedEl.removeChild(feedEl.firstChild);
}

setInterval(() => {
    const e = EVENTS[evIdx++ % EVENTS.length];
    addEvent(e.m, e.t, e.src);
}, 4000);

// 5. Expose specific functions to window for onclick handlers
window.setQ = function(el) {
    const qinput = document.getElementById('qinput');
    if(qinput) qinput.value = `Viability analysis: ${el.textContent} for India GPU semiconductor manufacturing`;
};

window.simulateFault = function() {
    addEvent('⚠ mcp_timeout | worker=logistics_coordinator | tool=get_port_data', 'err', 'DTB');
    setTimeout(() => addEvent('TKT-A3F7B2 | strategy=use_alternate_tool | fallback=trade-api-fallback', 'heal', 'THA'), 1000);
    setTimeout(() => addEvent('Healing complete | confidence adjusted', 'info', 'COG'), 2500);
    alert("Fault Simulated! Check Telemetry Hub.");
};

window.runQuery = function() {
    const resBox = document.querySelector('.res-box');
    if(resBox) resBox.style.display = 'block';
    const resBody = document.getElementById('res-body');
    if(resBody) resBody.innerHTML = "Executing query across 11 agents...\n[OK] Geological Expert\n[OK] Fab Locator\n[WARN] Logistics - Port Data Timeout\n[OK] THA Healed - Used Fallback\n\nBlueprint successfully synthesized.";
};
