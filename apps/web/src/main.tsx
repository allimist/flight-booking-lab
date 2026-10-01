import React,{useEffect,useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';

const API=import.meta.env.VITE_API_URL||'http://localhost:3020/api';
// Service addresses shown on the login page (host-side ports from docker-compose.yml).
const SERVICES=[
  {name:'API',value:API.replace(/\/api$/,''),href:true,usedFor:'Fastify REST API, JWT auth, outbox publisher, payment-timeout worker, aviationstack import'},
  {name:'PostgreSQL',value:import.meta.env.VITE_POSTGRES||'localhost:5435 (user/db/password: booking)',href:false,usedFor:'Source of truth: users, airports, flights, cabins, bookings, aviationstack request ledger, outbox events'},
  {name:'Redis',value:import.meta.env.VITE_REDIS||'redis://localhost:6380',href:false,usedFor:'Booked seats per departure (fs:{cabinId}:YYYY-MM) and the atomic Lua hold over every leg of a trip'},
  {name:'Kafka broker',value:import.meta.env.VITE_KAFKA_BROKER||'kafka:9092',href:false,usedFor:'Booking events published from the outbox (address inside the compose network)'},
  {name:'Kafka UI',value:import.meta.env.VITE_KAFKA_UI_URL||'http://localhost:8081',href:true,usedFor:'Browse topics and messages'},
];
async function api(path:string, opts:any={}) {
  const token=localStorage.getItem('token');
  const headers:any={...(opts.body?{'Content-Type':'application/json'}:{}),...(token?{Authorization:`Bearer ${token}`}:{})};
  const r=await fetch(API+path,{...opts,headers});
  const data=await r.json(); if(!r.ok) throw new Error(data.error||data.message||'Request failed'); return data;
}

const isoDate=(d:Date)=>d.toISOString().slice(0,10);
const plusDays=(iso:string,n:number)=>{const t=new Date(iso+'T00:00:00Z');t.setUTCDate(t.getUTCDate()+n);return isoDate(t)};
const today=isoDate(new Date());
/** Calendar-month shift that stays in the target month: 31 Jan + 1 month = 28/29 Feb, not 3 Mar. */
const plusMonths=(iso:string,n:number)=>{const [y,m,d]=iso.split('-').map(Number);const last=new Date(Date.UTC(y,m-1+n+1,0)).getUTCDate();return isoDate(new Date(Date.UTC(y,m-1+n,Math.min(d,last))))};
const SHIFTS:[string,number,'day'|'month'][]=[['−1 month',-1,'month'],['−1 week',-7,'day'],['−1 day',-1,'day'],['+1 day',1,'day'],['+1 week',7,'day'],['+1 month',1,'month']];
const fmt=(iso:string)=>new Date(iso.slice(0,10)+'T00:00:00Z').toLocaleDateString(undefined,{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
const fmtShort=(iso:string)=>new Date(iso.slice(0,10)+'T00:00:00Z').toLocaleDateString(undefined,{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'});
// Display currency. Prices are computed and charged in baht; other currencies are converted here with the fixed rates
// from /api/currencies (edited by the admin), so switching currency never calls an exchange-rate API.
type Currency={code:string,name?:string,symbol:string,thbPerUnit:number};
const THB:Currency={code:'THB',name:'Thai baht',symbol:'฿',thbPerUnit:1};
let CUR:Currency=THB; // set by App on every render from the selected currency
const inCur=(thb:any)=>Number(thb)/CUR.thbPerUnit;
const money=(thb:any)=>CUR.symbol+Math.round(inCur(thb)).toLocaleString();
/** Short form for dense tables: ฿26.6k, $806. */
const moneyShort=(thb:any)=>{const v=inCur(thb);return CUR.symbol+(v>=10000?`${Math.round(v/100)/10}k`:Math.round(v).toLocaleString())};
/** " (charged ฿26,600)" when showing another currency than baht. */
const charged=(thb:any)=>CUR.code==='THB'?'':` (charged ฿${Math.round(Number(thb)).toLocaleString()})`;
const daysBetween=(a:string,b:string)=>Math.round((Date.parse(b+'T00:00:00Z')-Date.parse(a+'T00:00:00Z'))/86400000);
/** "Same day", "1 day", "14 days": the calendar days between departure and return. */
const tripDays=(a:string,b:string)=>{const n=daysBetween(a,b);return n===0?'Same day':`${n} day${n===1?'':'s'}`};
const dur=(min:number)=>`${Math.floor(min/60)}h ${String(min%60).padStart(2,'0')}m`;
const STATUS_LABEL:Record<string,string>={PENDING:'Awaiting payment',CONFIRMED:'Confirmed',CANCELLED:'Cancelled',PAYMENT_TIMEOUT:'Payment timed out',EXPIRED:'Expired'};
const CABIN_LABEL:Record<string,string>={ECONOMY:'Economy',BUSINESS:'Business'};
const secondsLeft=(b:any)=>Math.max(0,Math.ceil((Date.parse(b.expiresAt)-Date.now())/1000));
const hoursUntil=(iso:string)=>Math.round((Date.parse(iso)-Date.now())/3600000);
/** Where a confirmed trip is relative to now, worded like an airline's trip list. */
function tripPhase(b:any){
  if(b.status!=='CONFIRMED'||!b.legs?.length)return null;
  const first=b.legs[0].depAt,last=b.legs[b.legs.length-1].arrAt,h=hoursUntil(first);
  if(Date.now()<Date.parse(first))return{key:'upcoming',label:'Upcoming',hint:h<48?`Departs in ${h} hours`:`Departs in ${Math.round(h/24)} days`};
  if(Date.now()<Date.parse(last))return{key:'current',label:'Travelling',hint:`Back ${fmt(last)}`};
  return{key:'completed',label:'Completed',hint:`Ended ${fmt(last)}`};
}

// Seeded accounts. Admin comes from database/init.sql; the rest are created by "Generate Sample Data".
const ACCOUNTS=[
  {label:'Admin',email:'admin@example.com',password:'admin123',role:'ADMIN'},
  {label:'Customer',email:'customer@example.com',password:'customer123',role:'CUSTOMER'},
  {label:'Customer 2',email:'customer2@example.com',password:'customer123',role:'CUSTOMER'},
  {label:'Seller',email:'seller@example.com',password:'seller123',role:'SELLER'},
  {label:'Seller 2',email:'seller2@example.com',password:'seller123',role:'SELLER'},
];
const ROLE_TABLES=[['ADMIN','Admins'],['SELLER','Sellers (airlines)'],['CUSTOMER','Customers']] as const;

const num=(n:any)=>Number(n).toLocaleString();
const ms=(n:any)=>`${Number(n).toLocaleString(undefined,{maximumFractionDigits:1})} ms`;

function Tile({label,value,hint,cls}:{label:string,value:any,hint?:any,cls?:string}){
  return <div className="tile"><div className="tile-label">{label}</div><div className={`tile-value ${cls||''}`}>{value}</div>{hint&&<div className="tile-hint">{hint}</div>}</div>;
}

/** Airport picker: "Tel Aviv (TLV)". */
function AirportSelect({label,airports,value,onChange,exclude}:{label:string,airports:any[],value:string,onChange:(v:string)=>void,exclude?:string}){
  return <label>{label}<select value={value} onChange={e=>onChange(e.target.value)}>
    {airports.filter(a=>a.iata!==exclude).map(a=><option key={a.iata} value={a.iata}>{a.city} ({a.iata}){a.country!=='Unknown'?` · ${a.country}`:''}</option>)}</select></label>;
}

/** One flight of an itinerary: times, airports, flight number, duration. */
function LegLine({l}:{l:any}){
  return <div className="leg">
    <div className="leg-time"><b>{l.depLocal}</b><small>{l.from}</small></div>
    <div className="leg-mid"><small>{l.airline} · {l.flightNumber}{l.aircraft?` · ${l.aircraft}`:''}</small><div className="line"/><small>{dur(l.durationMin)} · {fmtShort(l.date)}</small></div>
    <div className="leg-time"><b>{l.arrLocal}{l.arrDayOffset>0&&<sup>+{l.arrDayOffset}</sup>}</b><small>{l.to}</small></div>
  </div>;
}

/** A search result: legs with layovers, badges and the price. */
function ItineraryCard({it,passengers,onPick,picked,actionLabel,disabled}:{it:any,passengers:number,onPick?:()=>void,picked?:boolean,actionLabel?:string,disabled?:boolean}){
  return <article className={`card itin${picked?' picked':''}${it.soldOut?' dim':''}`}>
    <div className="itin-legs">
      {it.legs.map((l:any,i:number)=><React.Fragment key={l.cabinId+l.date}>
        {i>0&&<div className="layover">Layover in {it.layovers[i-1].city} ({it.layovers[i-1].airport}) · {dur(it.layovers[i-1].minutes)}</div>}
        <LegLine l={l}/></React.Fragment>)}
    </div>
    <div className="itin-side">
      <div>{it.stops===0?<span className="badge direct">Direct</span>:<span className="badge stop">1 stop via {it.via.join(', ')}</span>}
        {it.soldOut&&<span className="soldout">Sold out</span>}{it.source==='SAMPLE'&&<span className="tag" title="Synthetic flight: not from aviationstack">sample</span>}</div>
      {!it.soldOut&&<small className={`fare-tag ${it.fareClass}`}>{FARE_TAG[it.fareClass]} fare · {refundText(it.fares.find((f:any)=>f.code===it.fareClass)?.refundPct)}</small>}
      <div className="price-big" title={it.legs.map((l:any)=>`${l.flightNumber}: ${money(l.price)}${l.label?` (${l.label})`:''}`).join('\n')}>{money(it.pricePerPassenger)}<small> / passenger</small></div>
      {passengers>1&&<small>{money(it.totalPrice)} for {passengers}</small>}
      <small>{dur(it.durationMin)} total · {it.seatsLeft<20?<span className="notice">{it.seatsLeft} seats left</span>:`${it.seatsLeft} seats left`}</small>
      {onPick&&<button className={picked?'active':''} disabled={disabled||it.soldOut} onClick={onPick}>{actionLabel||'Select'}</button>}
    </div>
  </article>;
}

/** Cheapest price per day for the next 7 days; click a day to search it. */
/** Cheapest fare per day for 7 days around `date`; days before `minDate` (e.g. the return before the outbound) are disabled. */
function DateStrip({from,to,date,cabin,passengers,onPick,refreshKey,minDate=today}:{from:string,to:string,date:string,cabin:string,passengers:number,onPick:(d:string)=>void,refreshKey:number,minDate?:string}){
  const [days,setDays]=useState<any[]>([]);
  const start=plusDays(date,-3)<minDate?minDate:plusDays(date,-3);
  useEffect(()=>{if(!from||!to)return;api(`/flights/calendar?from=${from}&to=${to}&date=${start}&days=7&cabin=${cabin}&passengers=${passengers}`).then(setDays).catch(()=>setDays([]))},[from,to,start,cabin,passengers,refreshKey]);
  const low=Math.min(...days.filter(d=>d.cheapest&&d.date>=minDate).map(d=>d.cheapest));
  return <div className="datestrip">{days.map(d=><button key={d.date} className={`${d.date===date?'active':''}${d.cheapest===low?' low':''}`} onClick={()=>onPick(d.date)} disabled={!d.cheapest||d.date<minDate}>
    <small>{fmtShort(d.date)}</small><b>{d.cheapest?money(d.cheapest):'—'}</b></button>)}</div>;
}

/** Stacked bars: booked vs free seats per day. Inline SVG, no library. */
function SeatsChart({perDay}:{perDay:any[]}){
  const W=640,H=220,padL=44,padB=34,padT=10,gap=2,n=perDay.length;
  const plotW=W-padL-8,plotH=H-padT-padB,bw=Math.min(56,(plotW/n)-10);
  const max=Math.max(1,...perDay.map(d=>d.inventory));
  const y=(v:number)=>padT+plotH-(v/max)*plotH;
  const ticks=[0,0.25,0.5,0.75,1].map(f=>Math.round(max*f));
  return <figure className="chart">
    <figcaption><b>Seats per day</b> · booked vs free on your departures, next {n} days</figcaption>
    <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Booked and free seats per day">
      {ticks.map(t=><g key={t}><line x1={padL} x2={W-8} y1={y(t)} y2={y(t)} className="grid"/><text x={padL-6} y={y(t)+4} textAnchor="end" className="axis">{t}</text></g>)}
      {perDay.map((d,i)=>{const x=padL+(plotW/n)*i+((plotW/n)-bw)/2;const yb=y(d.booked),yf=y(d.booked+d.free);const bookedH=Math.max(0,padT+plotH-yb);
        return <g key={d.day}>
          <title>{`${fmt(d.day)}\n${d.departures} departures, ${d.inventory} seats\nBooked: ${d.booked} (${d.locked} held)\nFree: ${d.free}`}</title>
          <rect x={x} y={yf} width={bw} height={Math.max(0,yb-yf-(bookedH?gap:0))} rx={4} className="bar-free"/>
          {bookedH>0&&<rect x={x} y={yb} width={bw} height={bookedH} rx={bookedH>4?4:0} className="bar-booked"/>}
          {d.booked>0&&<text x={x+bw/2} y={Math.max(padT+10,yb-4)} textAnchor="middle" className="label">{d.booked}</text>}
          <text x={x+bw/2} y={H-padB+16} textAnchor="middle" className="axis">{new Date(d.day+'T00:00:00Z').toLocaleDateString(undefined,{weekday:'short',timeZone:'UTC'})}</text>
          <text x={x+bw/2} y={H-padB+29} textAnchor="middle" className="axis muted">{new Date(d.day+'T00:00:00Z').toLocaleDateString(undefined,{day:'numeric',month:'short',timeZone:'UTC'})}</text>
        </g>})}
    </svg>
    <div className="legend"><span><i className="sw booked"/>Booked (confirmed + held)</span><span><i className="sw free"/>Free</span></div>
  </figure>;
}

/** Horizontal bars: load factor per flight for the window. */
function FlightBars({flights}:{flights:any[]}){
  const rowH=26,W=640,labelW=170,valW=130,H=flights.length*rowH+8,plotW=W-labelW-valW;
  return <figure className="chart">
    <figcaption><b>Load factor by flight</b> · booked seats as % of seats flown</figcaption>
    <svg viewBox={`0 0 ${W} ${Math.max(H,30)}`} role="img" aria-label="Load factor per flight">
      {flights.map((f,i)=>{const y=4+i*rowH;const w=Math.max(0,plotW*f.loadFactorPct/100);
        return <g key={f.id}><title>{`${f.name} ${f.route}\n${f.bookedSeats} of ${f.capacitySeats} seats booked on ${f.departures} departures · ${f.bookings} bookings · ${money(f.revenue)}`}</title>
          <text x={labelW-8} y={y+rowH/2+4} textAnchor="end" className="axis">{f.name} {f.route}</text>
          <rect x={labelW} y={y+5} width={plotW} height={rowH-10} rx={4} className="bar-free"/>
          {w>0&&<rect x={labelW} y={y+5} width={w} height={rowH-10} rx={4} className="bar-booked"/>}
          <text x={labelW+plotW+8} y={y+rowH/2+4} className="label">{f.loadFactorPct}% · {f.freeSeats} free</text>
        </g>})}
    </svg>
  </figure>;
}

function SellerDashboard({days,setDays,onMessage}:{days:number,setDays:(n:number)=>void,onMessage:(m:string)=>void}){
  const [data,setData]=useState<any>(null); const [table,setTable]=useState(false);
  async function load(){try{setData(await api(`/seller/dashboard?days=${days}`))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[days]);
  if(!data) return <section className="card"><h2>Dashboard</h2><p>Loading…</p></section>;
  const t=data.tiles;
  return <section className="card">
    <div className="row between"><h2>Dashboard <small>{data.airlines.map((a:any)=>a.name).join(', ')||'no airlines yet'}</small></h2>
      <div className="row"><label>Range<select value={days} onChange={e=>setDays(Number(e.target.value))}><option value={7}>Next 7 days</option><option value={14}>Next 14 days</option><option value={30}>Next 30 days</option></select></label><button onClick={load}>Refresh</button><button onClick={()=>setTable(!table)}>{table?'Charts':'Table view'}</button></div></div>
    <div className="tiles">
      <Tile label="Flights" value={t.flights} hint={`${t.airlines} airline${t.airlines===1?'':'s'} · ${t.departures} departures`}/>
      <Tile label="Load factor" value={`${t.loadFactorPct}%`} hint={`${num(t.bookedSeats)} of ${num(t.capacitySeats)} seats`}/>
      <Tile label="Free seats" value={num(t.freeSeats)}/>
      <Tile label="Bookings" value={t.bookings} hint={`${t.lockedNow} seats held awaiting payment`}/>
      <Tile label="Revenue" value={money(t.revenue)} hint="confirmed, departing in the window"/>
    </div>
    {!table?<><SeatsChart perDay={data.perDay}/>{data.flights.length>0&&<FlightBars flights={data.flights}/>}</>:
    <div className="tablewrap">
      <table><thead><tr><th>Day</th><th>Departures</th><th>Seats</th><th>Booked</th><th>Held</th><th>Free</th></tr></thead><tbody>{data.perDay.map((d:any)=><tr key={d.day}><td>{fmt(d.day)}</td><td>{d.departures}</td><td>{d.inventory}</td><td>{d.booked}</td><td>{d.locked}</td><td>{d.free}</td></tr>)}</tbody></table>
      <table><thead><tr><th>Flight</th><th>Route</th><th>Departures</th><th>Bookings</th><th>Booked seats</th><th>Free seats</th><th>Load factor</th><th>Revenue</th></tr></thead><tbody>{data.flights.map((f:any)=><tr key={f.id}><td>{f.name}</td><td>{f.route}</td><td>{f.departures}</td><td>{f.bookings}</td><td>{f.bookedSeats}</td><td>{f.freeSeats}</td><td>{f.loadFactorPct}%</td><td>{money(f.revenue)}</td></tr>)}</tbody></table>
    </div>}
  </section>;
}

const WEEKDAYS=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const KIND_LABEL:Record<string,string>={SEASON:'Season',HOLIDAY:'Holiday',DISCOUNT:'Discount',WEEKDAY:'Day of week',DEMAND:'Demand',ADVANCE:'Booking time'};
const signedPct=(n:number)=>`${n>0?'+':n<0?'−':''}${Math.abs(n)}%`;
const ruleWhen=(r:any)=>r.start_date===r.end_date?fmt(r.start_date):`${fmt(r.start_date)} → ${fmt(r.end_date)}`;
const ruleAdjust=(r:any)=>r.kind==='DISCOUNT'?`−${r.adjust_value}%`:r.adjust_type==='FIXED'?`${money(r.adjust_value)} fixed`:signedPct(r.adjust_value);
const PRICE_ORDER='Price of a seat = base fare → holiday price (replaces season and day of week) or season price → × day-of-week % → × demand (≥50% sold +15%, ≥80% +40%) → × booking time (<7 days +30%, ≥60 days −10%) → − the biggest discount. Cabin rules beat flight rules beat airline rules beat country rules (departure country).';

/** Seven % boxes, Sunday..Saturday. With `inherited` (airline mode) an empty box uses the country's value. */
function WeekdayGrid({value,inherited,inheritedLabel,onSave}:{value:(number|null)[],inherited?:number[],inheritedLabel?:string,onSave:(v:(number|null)[])=>void}){
  const asText=(a:(number|null)[])=>a.map(x=>x==null?'':String(Number(x)));
  const [v,setV]=useState<string[]>(asText(value));
  useEffect(()=>setV(asText(value)),[asText(value).join()]);
  const put=(i:number,x:string)=>{const n=[...v];n[i]=x;setV(n)};
  return <div className="weekday-grid">{WEEKDAYS.map((d,i)=><label key={d}>{d}
      <input type="number" value={v[i]} placeholder={inherited?String(inherited[i]):'0'} onChange={e=>put(i,e.target.value)}/>
      {inherited&&<small>{v[i]===''?`${inheritedLabel||'country'} ${signedPct(inherited[i])}`:<a href="#" onClick={e=>{e.preventDefault();put(i,'')}}>use country</a>}</small>}</label>)}
    <button disabled={v.join()===asText(value).join()} onClick={()=>onSave(v.map(x=>x===''?null:Number(x)))}>Save</button></div>;
}

/** Add a season / holiday / discount, optionally for one flight or one cabin. Discounts are always "% off". */
function RuleForm({kinds,cabins,onAdd}:{kinds:string[],cabins?:any[],onAdd:(b:any)=>Promise<boolean>}){
  const [f,setF]=useState<any>({kind:kinds[0],name:'',target:'',startDate:'',endDate:'',adjustType:'PERCENT',adjustValue:20});
  const set=(k:string,v:any)=>setF({...f,[k]:v}); const disc=f.kind==='DISCOUNT';
  const flights=cabins?[...new Map(cabins.map(c=>[c.scheduleId,c])).values()]:[];
  const body=()=>{const [t,idv]=f.target.split(':');return {...f,adjustValue:Number(f.adjustValue),scheduleId:t==='f'?idv:undefined,cabinId:t==='c'?idv:undefined}};
  return <div className="rule-form">
    <label>Kind<select value={f.kind} onChange={e=>setF({...f,kind:e.target.value,adjustType:'PERCENT',adjustValue:e.target.value==='DISCOUNT'?10:20})}>{kinds.map(k=><option key={k} value={k}>{KIND_LABEL[k]}</option>)}</select></label>
    <label>Name<input value={f.name} onChange={e=>set('name',e.target.value)} placeholder={f.kind==='HOLIDAY'?'e.g. Passover':f.kind==='SEASON'?'e.g. High season':'e.g. Early bird'}/></label>
    {cabins&&<label>Applies to<select value={f.target} onChange={e=>set('target',e.target.value)}><option value="">All flights of the airline</option>
      {flights.map((c:any)=><option key={c.scheduleId} value={`f:${c.scheduleId}`}>{c.flightNumber} {c.from} → {c.to}</option>)}
      {cabins.map((c:any)=><option key={c.id} value={`c:${c.id}`}>{c.flightNumber} {CABIN_LABEL[c.cabin]} only</option>)}</select></label>}
    <label>From<input type="date" value={f.startDate} onChange={e=>setF({...f,startDate:e.target.value,endDate:f.endDate&&f.endDate>=e.target.value?f.endDate:e.target.value})}/></label>
    <label>To<input type="date" min={f.startDate} value={f.endDate} onChange={e=>set('endDate',e.target.value)}/></label>
    {!disc&&<label>Price<select value={f.adjustType} onChange={e=>set('adjustType',e.target.value)}><option value="PERCENT">% of base fare</option><option value="FIXED">Fixed price ฿</option></select></label>}
    <label>{disc?'Discount %':f.adjustType==='PERCENT'?'Change %':'Price ฿'}<input type="number" value={f.adjustValue} onChange={e=>set('adjustValue',e.target.value)}/></label>
    <button onClick={async()=>{if(await onAdd(body()))setF({...f,name:''})}}>Add {KIND_LABEL[f.kind].toLowerCase()}</button>
  </div>;
}

/** Seller: base fares, day-of-week % (overriding the country), seasons / holidays / discounts, and a 60-day fare calendar. */
function PricesModal({airline,onClose,onMessage}:{airline:any,onClose:()=>void,onMessage:(m:string)=>void}){
  const [d,setD]=useState<any>(null); const [base,setBase]=useState<Record<string,string>>({});
  async function load(){try{const x=await api(`/seller/airlines/${airline.iata}/prices?days=60`);setD(x);setBase(Object.fromEntries(x.cabins.map((c:any)=>[c.id,String(Math.round(Number(c.price)))])))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[airline.iata]);
  async function call(path:string,method:string,body:any,done:string){try{await api(path,{method,...(body!==undefined?{body:JSON.stringify(body)}:{})});onMessage(done);load();return true}catch(e:any){onMessage(e.message);return false}}
  const firstCountry=d?.countries?.[0];
  return <div className="modal" onClick={onClose}><div className="card wide" onClick={e=>e.stopPropagation()}><button onClick={onClose}>Close</button>
    <h2>Prices · {airline.name} <small>({airline.iata})</small></h2>
    {!d?<p>Loading…</p>:<>
    <small>{PRICE_ORDER}</small>
    <h3>Base fare per seat <small>(set in baht)</small></h3>
    <div className="tablewrap"><table><tbody>{d.cabins.map((c:any)=><tr key={c.id}><td><b>{c.flightNumber}</b> {c.from} → {c.to}</td><td>{CABIN_LABEL[c.cabin]}</td><td>{c.totalSeats} seats</td>
      <td><input type="number" min={1} value={base[c.id]??''} onChange={e=>setBase({...base,[c.id]:e.target.value})}/></td>
      <td><button onClick={()=>call(`/seller/cabins/${c.id}`,'PATCH',{price:Number(base[c.id])},`Base fare of ${c.flightNumber} ${CABIN_LABEL[c.cabin]} saved`)} disabled={Number(base[c.id])===Math.round(Number(c.price))}>Save</button></td></tr>)}</tbody></table></div>
    <h3>Day of the week</h3>
    <small>Leave a day empty to use the departure country's default (set by the admin; placeholders show {firstCountry}); fill it in to use your own % for that day on every flight of {airline.name}. Negative = cheaper.</small>
    <WeekdayGrid value={d.airlinePct} inherited={d.countryPct[firstCountry]} inheritedLabel={firstCountry} onSave={pct=>call(`/seller/airlines/${airline.iata}/weekdays`,'PUT',{pct},'Day-of-week prices saved')}/>
    <h3>Seasons, holidays and discounts</h3>
    <div className="tablewrap"><table><thead><tr><th>Kind</th><th>Name</th><th>Applies to</th><th>When</th><th>Price</th><th></th></tr></thead><tbody>
      {d.rules.map((r:any)=><tr key={r.id} className={r.source==='country'?'inherited':''}><td><span className={`ptag ${r.kind}`}>{KIND_LABEL[r.kind]}</span></td><td>{r.name}</td>
        <td>{r.source==='country'?`Departures from ${r.country} (country default)`:r.target||'All flights'}</td><td>{ruleWhen(r)}</td><td>{ruleAdjust(r)}</td>
        <td>{r.source!=='country'&&<button className="danger" onClick={()=>confirm(`Delete "${r.name}"?`)&&call(`/seller/price-rules/${r.id}`,'DELETE',undefined,'Rule deleted')}>Delete</button>}</td></tr>)}
      {d.rules.length===0&&<tr><td colSpan={6}>No seasons, holidays or discounts.</td></tr>}
    </tbody></table></div>
    <RuleForm kinds={['SEASON','HOLIDAY','DISCOUNT']} cabins={d.cabins} onAdd={b=>call(`/seller/airlines/${airline.iata}/price-rules`,'POST',b,`${KIND_LABEL[b.kind]} added`)}/>
    <h3>Next {d.days} days</h3>
    <div className="tablewrap"><table className="price-cal"><thead><tr><th>Flight</th>{d.calendar[0]?.daily.map((n:any)=>{const dt=new Date(n.date+'T00:00:00Z');
      return <th key={n.date}>{dt.getUTCDate()===1||n.date===d.from?<small>{dt.toLocaleDateString(undefined,{month:'short',timeZone:'UTC'})}<br/></small>:null}{WEEKDAYS[dt.getUTCDay()].slice(0,2)}<br/>{dt.getUTCDate()}</th>})}</tr></thead><tbody>
      {d.calendar.map((c:any)=>{const cab=d.cabins.find((x:any)=>x.id===c.cabinId);return <tr key={c.cabinId}><td>{cab?.flightNumber} <small>{CABIN_LABEL[cab?.cabin]}</small></td>
        {c.daily.map((n:any)=>n.price==null?<td key={n.date} className="muted">·</td>:<td key={n.date} className={n.rule?`p-${n.rule.kind}${n.price<Number(cab?.price)?' cheaper':''}`:''} title={`${fmt(n.date)}: ${money(n.price)}${n.label?` (${n.label})`:' (base fare)'}`}>{moneyShort(n.price)}</td>)}</tr>})}
    </tbody></table></div>
    <small className="legend"><span className="p-WEEKDAY">day of week</span> <span className="p-SEASON">season</span> <span className="p-HOLIDAY">holiday</span> <span className="p-DEMAND">demand</span> <span className="p-ADVANCE">booking time</span> <span className="p-DISCOUNT">discount</span> · colour = strongest layer · <b>bold</b> = below base fare · hover a price for its layers · price per seat in {CUR.code} · “·” = no flight that day</small>
    </>}
  </div></div>;
}

/** Admin: fixed display rates (baht per unit). Prices stay in baht; these only change what customers see. */
function CurrencyRates({currencies,onChange,onMessage}:{currencies:Currency[],onChange:()=>void,onMessage:(m:string)=>void}){
  const [v,setV]=useState<Record<string,string>>({});
  useEffect(()=>setV(Object.fromEntries(currencies.map(c=>[c.code,String(c.thbPerUnit)]))),[currencies]);
  async function save(code:string){try{await api(`/admin/currencies/${code}`,{method:'PUT',body:JSON.stringify({thbPerUnit:Number(v[code])})});onMessage(`${code} rate saved`);onChange()}catch(e:any){onMessage(e.message)}}
  return <section className="card admin"><h2>Display currencies</h2>
    <small>Prices are computed, stored and charged in baht. Customers can view them in another currency, converted with these fixed rates; nothing calls an exchange-rate API, so update them by hand when you like.</small>
    <div className="tablewrap"><table><thead><tr><th>Currency</th><th>1 unit =</th><th>Updated</th><th></th></tr></thead><tbody>
      {currencies.map(c=><tr key={c.code}><td><b>{c.symbol} {c.code}</b> <small>{c.name}</small></td>
        <td>{c.code==='THB'?'฿1 (base)':<><input type="number" step="0.01" min={0} value={v[c.code]??''} onChange={e=>setV({...v,[c.code]:e.target.value})}/> ฿</>}</td>
        <td><small>{(c as any).updatedAt?new Date((c as any).updatedAt).toLocaleDateString():''}</small></td>
        <td>{c.code!=='THB'&&<button disabled={Number(v[c.code])===c.thbPerUnit} onClick={()=>save(c.code)}>Save</button>}</td></tr>)}
    </tbody></table></div>
  </section>;
}

/** Admin: each departure country's day-of-week % and national seasons / holidays. Airlines inherit them unless they override. */
function CountryPricing({onMessage}:{onMessage:(m:string)=>void}){
  const [rows,setRows]=useState<any[]|null>(null);
  async function load(){try{setRows(await api('/admin/country-pricing'))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[]);
  async function call(path:string,method:string,body:any,done:string){try{await api(path,{method,...(body!==undefined?{body:JSON.stringify(body)}:{})});onMessage(done);load();return true}catch(e:any){onMessage(e.message);return false}}
  return <section className="card"><h2>Country pricing</h2>
    <small>{PRICE_ORDER} Airlines can override single days and add their own seasons, holidays and discounts in My flights → Prices.</small>
    {!rows?<p>Loading…</p>:rows.map(c=><div key={c.country} className="country-card">
      <h3>{c.country} <small>{c.flights} flights depart from here</small></h3>
      <WeekdayGrid value={c.weekdayPct} onSave={pct=>call(`/admin/country-pricing/${encodeURIComponent(c.country)}/weekdays`,'PUT',{pct:pct.map(x=>x??0)},`${c.country}: day-of-week prices saved`)}/>
      {c.rules.length>0&&<div className="tablewrap"><table><tbody>{c.rules.map((r:any)=><tr key={r.id}><td><span className={`ptag ${r.kind}`}>{KIND_LABEL[r.kind]}</span></td><td>{r.name}</td><td>{ruleWhen(r)}</td><td>{ruleAdjust(r)}</td>
        <td><button className="danger" onClick={()=>confirm(`Delete "${r.name}" for ${c.country}?`)&&call(`/admin/price-rules/${r.id}`,'DELETE',undefined,'Rule deleted')}>Delete</button></td></tr>)}</tbody></table></div>}
      <RuleForm kinds={['SEASON','HOLIDAY']} onAdd={b=>call(`/admin/country-pricing/${encodeURIComponent(c.country)}/rules`,'POST',b,`${c.country}: ${KIND_LABEL[b.kind].toLowerCase()} added`)}/>
    </div>)}
  </section>;
}

/** Admin: aviationstack import plan, monthly budget and the request ledger (every request is made at most once). */
function FlightData({onMessage,onImported}:{onMessage:(m:string)=>void,onImported:()=>void}){
  const [d,setD]=useState<any>(null); const [busy,setBusy]=useState(false); const [last,setLast]=useState<any>(null);
  async function load(){try{setD(await api('/admin/aviationstack'))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[]);
  async function run(){
    const need=d.requestsNeeded;
    if(need&&!confirm(`This makes up to ${need} real aviationstack request(s) (${d.budget.left} of this month's ${d.budget.budget} left). Cached steps cost nothing. Continue?`))return;
    setBusy(true);try{const x=await api('/admin/aviationstack/import',{method:'POST',body:JSON.stringify({confirm:true})});setLast(x);onMessage(x.message);onImported()}catch(e:any){onMessage(e.message)}finally{setBusy(false);load()}}
  async function retry(key:string){if(!confirm(`Spend one more request on ${key}?`))return;try{onMessage((await api('/admin/aviationstack/retry',{method:'POST',body:JSON.stringify({key})})).message);onImported()}catch(e:any){onMessage(e.message)}finally{load()}}
  if(!d) return <section className="card"><h2>Flight data</h2><p>Loading…</p></section>;
  const b=d.budget,pct=(a:number,c:number)=>c?Math.min(100,a/c*100):0;
  return <section className="card admin">
    <div className="row between"><h2>Flight data · aviationstack</h2><button onClick={load}>Refresh</button></div>
    <small>The free plan allows {b.planMonthly} requests a month and has no future schedules, so real flights seen today become daily schedules for the next year. Every request is identified by endpoint + parameters and made <b>at most once</b>: the response is kept in PostgreSQL and in a snapshot file (<code>data/aviationstack</code>), so a new database rebuilds the catalogue without calling the API.</small>
    <div className="tiles">
      <div className="tile"><div className="tile-label">Requests this month</div><div className="tile-value">{b.used} / {b.budget}</div><div className="meter"><div style={{width:`${pct(b.used,b.budget)}%`}}/></div><div className="tile-hint">lab budget (AVIATIONSTACK_MONTHLY_BUDGET) of the plan's {b.planMonthly} · {b.failed} failed · {b.allTime} all time</div></div>
      <Tile label="Needed for the plan" value={d.requestsNeeded} cls={d.requestsNeeded?'':'ok'} hint={d.requestsNeeded?(d.hubsPending?'hubs are chosen after step 3, so up to this many':'uncached steps'):'everything is cached: importing is free'}/>
      <Tile label="API key" value={b.keyConfigured?'✓ Set':'✕ Not set'} cls={b.keyConfigured?'ok':'bad'} hint={b.keyConfigured?b.baseUrl:'only snapshots can be imported'}/>
      <Tile label="Hubs" value={d.hubs?d.hubs.join(' · '):'—'} hint={d.hubs?'busiest connections from TLV departures':'found by step 3'}/>
    </div>
    <div className="row"><button onClick={run} disabled={busy}>{busy?'Importing…':d.requestsNeeded?`Import (up to ${d.requestsNeeded} request${d.requestsNeeded===1?'':'s'})`:'Rebuild flights from cache (0 requests)'}</button></div>
    <h3>Import plan</h3>
    <div className="tablewrap"><table><thead><tr><th>#</th><th>Step</th><th>Request</th><th>Status</th><th>Rows</th></tr></thead><tbody>
      {d.steps.map((s:any,i:number)=><tr key={s.key} className={s.cached?.status==='FAILED'?'warn':''}><td>{i+1}</td><td>{s.step}</td><td><code className="key">{s.key}</code></td>
        <td>{!s.cached?<span className="notice">1 request</span>:s.cached.status==='OK'?<span className="ok-text">✓ cached{s.cached.source==='SNAPSHOT'?' (snapshot)':''}</span>:<><span className="notice">✕ {s.cached.status}</span> <button onClick={()=>retry(s.key)}>Retry</button></>}</td>
        <td>{s.cached?.rows??'—'}</td></tr>)}
      {d.hubsPending&&<tr><td>…</td><td colSpan={4}><small>Then 4 requests for each of the 2 busiest hubs found in step 3 (TLV → hub, hub → BKK, BKK → hub, hub → TLV).</small></td></tr>}
    </tbody></table></div>
    {last&&<><h3>Last import</h3><div className="tablewrap"><table><tbody>{last.results.map((r:any)=><tr key={r.key}><td>{r.step}</td><td>{r.ok?(r.called?'called':'from cache'):<span className="notice">{r.error}</span>}</td><td>{r.rows??''}</td></tr>)}</tbody></table></div></>}
    <h3>Request ledger <small>(one row per distinct request)</small></h3>
    <div className="tablewrap"><table><thead><tr><th>Request</th><th>Status</th><th>Source</th><th>Calls</th><th>Rows</th><th>Flights built</th><th>When</th></tr></thead><tbody>
      {d.ledger.map((r:any)=><tr key={r.key} className={r.status==='OK'?'':'warn'}><td><code className="key">{r.key}</code>{r.error&&<><br/><small className="notice">{r.error}</small></>}</td><td>{r.status}</td><td>{r.source}</td><td>{r.calls}</td><td>{r.rows??'—'}</td><td>{r.schedules}</td><td>{new Date(r.createdAt).toLocaleString()}</td></tr>)}
      {d.ledger.length===0&&<tr><td colSpan={7}>No requests yet.</td></tr>}
    </tbody></table></div>
  </section>;
}

function SimulationPanel({onMessage,flights}:{onMessage:(m:string)=>void,flights:any[]}){
  const [mode,setMode]=useState<'random'|'same-flight'>('random');
  const [customers,setCustomers]=useState(100); const [payPct,setPayPct]=useState(50); const [latePct,setLatePct]=useState(30); const [windowS,setWindowS]=useState(5); const [pax,setPax]=useState(2);
  const cabins=flights.flatMap((f:any)=>(f.cabins||[]).map((c:any)=>({...c,flight:f})));
  const [cabinId,setCabinId]=useState(''); const [date,setDate]=useState(plusDays(today,7));
  const [runs,setRuns]=useState<any[]>([]); const [run,setRun]=useState<any>(null); const [busy,setBusy]=useState(false); const [showLog,setShowLog]=useState(false);
  useEffect(()=>{if(!cabins.find(c=>c.id===cabinId))setCabinId(cabins[0]?.id||'')},[flights]);
  async function loadRuns(){try{setRuns(await api('/admin/simulation'))}catch(e:any){onMessage(e.message)}}
  async function loadRun(id:string){try{const r=await api(`/admin/simulation/${id}`);setRun(r);return r}catch(e:any){onMessage(e.message)}}
  async function start(){if(payPct+latePct>100){onMessage('Pay % + late % cannot exceed 100');return}setBusy(true);setShowLog(false);
    try{const x=await api('/admin/simulation',{method:'POST',body:JSON.stringify({mode,customers,payRatio:payPct/100,lateRatio:latePct/100,windowSeconds:windowS,passengers:pax,cabinId,date})});onMessage(`Simulation started: ${x.params.customers} customers`);
      let r=await loadRun(x.runId);while(r&&r.status==='RUNNING'){await new Promise(res=>setTimeout(res,1000));r=await loadRun(x.runId)}
      onMessage(r?.status==='DONE'?'Simulation finished':'Simulation failed: '+(r?.report?.error||''));loadRuns()}
    catch(e:any){onMessage(e.message)}finally{setBusy(false)}}
  async function cancelAll(){if(!run)return;if(!confirm(`Cancel every active booking made by the ${run.customers} customers of this run?`))return;
    try{const x=await api(`/admin/simulation/${run.id}/cancel-all`,{method:'POST'});onMessage(x.message);loadRun(run.id);loadRuns()}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{loadRuns()},[]);
  const r=run?.report||{};const c=r.counts||{};
  return <>
    <h3>Load simulation: {customers} customers book at once</h3>
    <div className="row">
      <label>Scenario<select value={mode} onChange={e=>setMode(e.target.value as any)}>
        <option value="random">Random TLV ⇄ BKK trips, next 14 days · pay / late / abandon mix</option>
        <option value="same-flight">Same flight · fallback to another option that day, then the next day</option></select></label>
      <label>Customers<input type="number" min={1} max={500} value={customers} onChange={e=>setCustomers(Number(e.target.value))}/></label>
      <label>Passengers each<input type="number" min={1} max={9} value={pax} onChange={e=>setPax(Number(e.target.value))}/></label>
      {mode==='random'&&<>
        <label>Pay immediately %<input type="number" min={0} max={100} value={payPct} onChange={e=>setPayPct(Number(e.target.value))}/></label>
        <label>Late (expire, then rebook) %<input type="number" min={0} max={100} value={latePct} onChange={e=>setLatePct(Number(e.target.value))}/></label>
        <label>Payment window (s)<input type="number" min={3} max={60} value={windowS} onChange={e=>setWindowS(Number(e.target.value))}/></label></>}
      {mode==='same-flight'&&<>
        <label>Target flight<select value={cabinId} onChange={e=>setCabinId(e.target.value)}>{cabins.map(x=><option key={x.id} value={x.id}>{x.flight.flightNumber} {x.flight.from} → {x.flight.to} · {CABIN_LABEL[x.cabin]} · {x.totalSeats} seats</option>)}</select></label>
        <label>Date<input type="date" min={today} value={date} onChange={e=>setDate(e.target.value)}/></label></>}
      <button onClick={start} disabled={busy}>{busy?'Running…':'Run simulation'}</button>
      {runs.length>0&&<label>Previous runs<select value={run?.id||''} onChange={e=>e.target.value&&loadRun(e.target.value)}><option value="">Select…</option>{runs.map(x=><option key={x.id} value={x.id}>{new Date(x.createdAt).toLocaleTimeString()} · {x.customers} customers · {x.status} · {x.activeBookings} active</option>)}</select></label>}
    </div>
    {mode==='random'?<small>Each customer books a random one-way TLV ⇄ BKK itinerary (direct or with a connection) for {pax} passenger(s) in the next 14 days, all in the same instant. {payPct}% pay at once, {latePct}% let the {windowS}s hold expire and then try to rebook, the rest abandon. Customers rejected as sold out retry once on another trip. Every step is timed.</small>
    :<small>All {customers} customers try to book {pax} seat(s) on the <b>same departure</b> in the same instant. A rejected customer tries up to 3 other itineraries on the same route that day (other flights, connections), then up to 3 the next day. Whoever gets seats pays immediately.</small>}
    {run&&run.status==='RUNNING'&&<p className="lockinfo">Running… {r.phase||'booking burst'}</p>}
    {run&&run.status==='FAILED'&&<p className="notice">Failed: {r.error}</p>}
    {run&&run.status==='DONE'&&<div className="report">
      <div className="row between"><h4>Report · {new Date(run.createdAt).toLocaleString()} · {run.customers} customers</h4>
        <div className="row"><button className="danger" onClick={cancelAll} disabled={!run.activeBookings}>Cancel all {run.activeBookings} active bookings of these customers</button><button onClick={()=>loadRun(run.id)}>Refresh</button></div></div>
      {r.mode==='same-flight'&&r.target&&<p><small>Target: <b>{r.target.flight}</b> {r.target.from} → {r.target.to}, {CABIN_LABEL[r.target.cabin]}, {r.target.totalSeats} seats, {fmt(r.target.date)}, {r.target.passengers} seat(s) per customer. {r.target.sameDayOptions} other options that day, {r.target.nextDayOptions} the next day.</small></p>}
      <div className="tiles">
        <Tile label="Total time" value={`${(r.durationMs/1000).toFixed(1)} s`} hint={r.mode==='same-flight'?'incl. outbox drain':'incl. waiting for holds to expire'}/>
        <Tile label="Booking burst" value={ms(r.burstMs)} hint={`${r.throughputPerSec} ${r.mode==='same-flight'?'attempts':'bookings'}/s`}/>
        {r.mode==='same-flight'?<>
          <Tile label="Got target flight" value={r.tiers.target} hint={`${r.target?.totalSeats} seats`}/>
          <Tile label="Other option, same day" value={r.tiers.sameDay} hint="fallback 1"/>
          <Tile label="Next day" value={r.tiers.nextDay} hint="fallback 2"/>
          <Tile label="Nothing" value={r.tiers.none} hint="gave up"/>
          <Tile label="Attempts" value={r.totalAttempts} hint={`${r.avgAttemptsPerCustomer} per customer`}/>
        </>:<>
          <Tile label="Held" value={c.locked} hint={`${c.soldOut} sold out at first try`}/>
          <Tile label="Paid" value={c.paid} hint={`${c.payFailed} payments failed`}/>
          <Tile label="Timed out" value={c.timedOut} hint={`${c.late} late + ${c.abandoned} abandoned`}/>
          <Tile label="Rebooked" value={c.rebooked} hint={`${c.rebookSoldOut} lost the seats`}/>
        </>}
        <Tile label="Active now" value={run.activeBookings} hint="confirmed + pending"/>
        <Tile label="Errors" value={c.errors}/>
      </div>
      {r.mode==='same-flight'&&<>
        <h4>Where the customers ended up</h4>
        <div className="tablewrap"><table><thead><tr><th>Trip</th><th>Customers</th></tr></thead><tbody>{(r.tripsWon||[]).slice(0,15).map((x:any)=><tr key={x.trip}><td>{x.trip}</td><td>{x.count}</td></tr>)}</tbody></table></div>
      </>}
      <h4>Bottlenecks (slowest step first, p95)</h4>
      <div className="tablewrap"><table><thead><tr><th>Step</th><th>p95</th><th>Why</th></tr></thead><tbody>
        {(r.bottlenecks||[]).map((b:any)=><tr key={b.step} className={b.p95Ms>1000?'warn':''}><td>{b.step}</td><td>{ms(b.p95Ms)}</td><td><small>{b.note}</small></td></tr>)}
      </tbody></table></div>
      <p><small>{r.mode!=='same-flight'&&<>Inside one booking request, Redis took {r.redisShareOfBookPct}% and PostgreSQL {r.pgShareOfBookPct}% of the time. </>}Outbox: {r.outbox?.published}/{r.outbox?.events} events published, drained in {((r.outbox?.drainMs||0)/1000).toFixed(1)} s, avg lag {ms(r.outbox?.avgMs||0)}.</small></p>
      <h4>Timings per operation</h4>
      <div className="tablewrap"><table><thead><tr><th>Operation</th><th>Count</th><th>Avg</th><th>p50</th><th>p95</th><th>Max</th></tr></thead><tbody>
        {Object.entries(r.timings||{}).filter(([,v]:any)=>v.count).map(([k,v]:any)=><tr key={k}><td>{k}</td><td>{v.count}</td><td>{ms(v.avgMs)}</td><td>{ms(v.p50Ms)}</td><td>{ms(v.p95Ms)}</td><td>{ms(v.maxMs)}</td></tr>)}
      </tbody></table></div>
      <h4>What happened to the customers</h4>
      <div className="tablewrap"><table><thead><tr><th>Outcome</th><th>Customers</th></tr></thead><tbody>
        {(r.outcomes||[]).map((o:any)=><tr key={o.outcome}><td>{o.outcome}</td><td>{o.count}</td></tr>)}
        <tr><td><b>Final booking statuses</b></td><td>{Object.entries(r.finalStatuses||{}).map(([k,v]:any)=>`${STATUS_LABEL[k]||k}: ${v}`).join(' · ')}</td></tr>
      </tbody></table></div>
      <button onClick={()=>setShowLog(!showLog)}>{showLog?'Hide':'Show'} timeline</button>
      {showLog&&<pre className="log">{(r.log||[]).join('\n')}</pre>}
    </div>}
  </>;
}

/** 1536 -> "1.5 KB" */
function bytes(n:number){const u=['B','KB','MB','GB'];let i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return `${n.toFixed(i?1:0)} ${u[i]}`}

function RedisRecords({onMessage}:{onMessage:(m:string)=>void}){
  const [scheduleId,setScheduleId]=useState(''); const [date,setDate]=useState(''); const [page,setPage]=useState(1);
  const [data,setData]=useState<any>(null); const [map,setMap]=useState<any>(null);
  async function showMap(x:any){try{setMap(await api(`/flights/seatmap?cabinId=${x.cabinId}&date=${x.date}`))}catch(e:any){onMessage(e.message)}}
  const limit=50;
  async function load(){try{setData(await api(`/admin/redis-records?page=${page}&limit=${limit}${scheduleId?`&scheduleId=${scheduleId}`:''}${date?`&date=${date}`:''}`))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[scheduleId,date,page]);
  if(!data) return <section className="card"><h2>Redis records</h2><p>Loading…</p></section>;
  const s=data.summary, pages=Math.max(1,Math.ceil(data.total/limit));
  return <section className="card admin">
    <div className="row between"><h2>Redis records</h2><button onClick={load}>Refresh</button></div>
    <div className="tiles">
      <Tile label="Keys in Redis" value={s.totalKeys.toLocaleString()} hint={`one per cabin and month that has bookings, for ${s.cabins} cabins`}/>
      <Tile label="Booked seats" value={s.bookedSeats.toLocaleString()} cls="small" hint={`on ${s.checkedDepartures.toLocaleString()} departures in PostgreSQL (next ${s.windowDays} days)`}/>
      <Tile label="Redis vs PostgreSQL" value={s.mismatches?`✕ ${s.mismatches} differ`:'✓ Match'} cls={`small ${s.mismatches?'bad':'ok'}`} hint="booked seats of every booked departure compared"/>
      <Tile label="Seat bitmaps" value={s.seatMismatches?`✕ ${s.seatMismatches} differ`:'✓ Match'} cls={`small ${s.seatMismatches?'bad':'ok'}`} hint="BITCOUNT of each departure's seat bitmap = seats booked"/>
      <Tile label="Loaded" value={s.loadedAt?'✓ Yes':'✕ No'} cls={`small ${s.loadedAt?'ok':'bad'}`} hint={s.loadedAt?`rebuilt ${new Date(s.loadedAt).toLocaleString()}`:'bookings are refused until the rebuild runs'}/>
      <Tile label="Redis RAM" value={bytes(s.memory.usedBytes)} cls="small" hint={`data ${bytes(s.memory.datasetBytes)} · peak ${bytes(s.memory.peakBytes)} · limit ${s.memory.maxBytes?bytes(s.memory.maxBytes):'none'}`}/>
    </div>
    <div className="row">
      <select value={scheduleId} onChange={e=>{setScheduleId(e.target.value);setPage(1)}}><option value="">All flights</option>{data.flights.map((f:any)=><option key={f.id} value={f.id}>{f.name}</option>)}</select>
      <label>Date<input type="date" value={date} onChange={e=>{setDate(e.target.value);setPage(1)}}/></label>
      {date&&<button onClick={()=>{setDate('');setPage(1)}}>Clear date</button>}
    </div>
    <small>Redis stores only booked departures: a day with nothing booked has no entry, which means every seat is free. Days a flight does not fly are not listed. Each booked departure also has a seat bitmap (fsm:&#123;cabinId&#125;:date, one bit per seat); "map" shows it.</small>
    {map&&<div className="modal" onClick={()=>setMap(null)}><div className="card" onClick={e=>e.stopPropagation()}><button onClick={()=>setMap(null)}>Close</button>
      <h2>{map.flightNumber} · {CABIN_LABEL[map.cabin]} · {fmt(map.date)}</h2><p><small>{map.booked} booked · bitmap <code className="key">{map.redisKey}</code> · {map.fares.map((f:any)=>`${f.name} ${f.onSale?'on sale':'closed'} (cap ${f.cap})`).join(' · ')}</small></p>
      <SeatMap map={map}/></div></div>}
    <div className="tablewrap"><table><thead><tr><th>Date</th><th>Flight</th><th>Route</th><th>Cabin</th><th>Booked</th><th>Free</th><th>Seats</th><th>Seat bits</th><th>Redis key · field</th></tr></thead><tbody>
      {data.items.map((x:any)=><tr key={x.key} className={x.available===0?'warn':''}><td>{fmt(x.date)}</td><td>{x.flight}</td><td>{x.route}</td><td>{CABIN_LABEL[x.cabin]}</td><td>{x.stored?x.booked:<small>— (no entry)</small>}</td><td>{x.available}</td><td>{x.totalSeats}</td>
        <td className={x.seatBits!==x.booked?'notice':''}>{x.seatBits}{x.booked>0&&<> <button className="link" onClick={()=>showMap(x)}>map</button></>}</td><td><code className="key">{x.key}</code></td></tr>)}
      {data.items.length===0&&<tr><td colSpan={9}>No departures match.</td></tr>}
    </tbody></table></div>
    <div className="row between"><small>{data.total.toLocaleString()} departures · page {page} of {pages}</small>
      <span className="row"><button disabled={page<=1} onClick={()=>setPage(page-1)}>Previous</button><button disabled={page>=pages} onClick={()=>setPage(page+1)}>Next</button></span></div>
  </section>;
}

/** "24 Sep 2026 (6 keys)" for one or a few days, a range for many. */
function dayList(n:{night:string,keys:number}[]){
  if(!n.length) return '—';
  if(n.length<=3) return n.map(x=>`${fmt(x.night)} (${x.keys} keys)`).join(', ');
  return `${n.length} days: ${fmt(n[0].night)} → ${fmt(n[n.length-1].night)}`;
}
/** Runs of the availability worker: which departure days it removed from Redis. */
function AvailabilityLog({onMessage}:{onMessage:(m:string)=>void}){
  const [rows,setRows]=useState<any[]|null>(null);
  async function load(){try{setRows(await api('/admin/availability-log'))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[]);
  return <section className="card admin">
    <div className="row between"><h2>Availability log</h2><button onClick={load}>Refresh</button></div>
    <small>The availability worker runs at startup and just after every UTC midnight: it removes booked departure days that are already past from Redis. It never adds anything; bookings write their own days, and whole month keys also expire two days after their month ends.</small>
    {!rows?<p>Loading…</p>:rows.length===0?<p>No runs yet. Is the availability-worker service running?</p>:
    <div className="tablewrap"><table><thead><tr><th>Time</th><th>Trigger</th><th>Status</th><th>Window</th><th>Removed</th><th>Duration</th></tr></thead><tbody>
      {rows.map(r=><tr key={r.id} className={r.status==='OK'?'':'warn'}>
        <td>{new Date(r.ranAt).toLocaleString()}</td><td>{r.trigger}</td>
        <td>{r.status==='OK'?'OK':<span className="notice" title={r.error}>✕ Failed</span>}{r.error&&<><br/><small>{r.error}</small></>}</td>
        <td>{r.windowFirst?`${fmt(r.windowFirst)} → ${fmt(r.windowLast)}`:'—'}</td>
        <td><b>{r.removedKeys}</b> entries<br/><small>{dayList(r.removedNights)}</small></td>
        <td>{r.durationMs} ms</td>
      </tr>)}
    </tbody></table></div>}
  </section>;
}

const FARE_TAG:Record<string,string>={SAVER:'Saver',STANDARD:'Standard',FLEX:'Flex'};
const refundText=(pct:number)=>pct===0?'No refund':pct===100?'Full refund':`${pct}% refund`;
/** Seat label for an index in a layout ({letters, firstRow}); mirrors seatLabel() in the API. */
const seatAt=(L:any,i:number)=>`${L.firstRow+Math.floor(i/L.letters.length)}${L.letters[i%L.letters.length]}`;

/** Clickable seat grid. Taken seats are greyed out; up to `max` seats can be picked (none when read-only). */
function SeatMap({map,selected=[],max=0,onChange}:{map:any,selected?:string[],max?:number,onChange?:(s:string[])=>void}){
  const L=map.layout,taken=new Set(map.taken);
  const toggle=(seat:string)=>{if(!onChange)return;if(selected.includes(seat))onChange(selected.filter(x=>x!==seat));else if(selected.length<max)onChange([...selected,seat]);else onChange([...selected.slice(1),seat])};
  return <div className="seatmap"><div className="seat-head"><span/>{L.letters.map((l:string,i:number)=><React.Fragment key={l}><span>{l}</span>{L.aisleAfter.includes(i)&&<span className="aisle"/>}</React.Fragment>)}</div>
    {Array.from({length:L.rows},(_,r)=><div className="seat-row" key={r}><span className="rowno">{L.firstRow+r}</span>
      {L.letters.map((l:string,c:number)=>{const i=r*L.letters.length+c,seat=seatAt(L,i);
        return <React.Fragment key={l}>{i>=L.total?<span className="seat none"/>:
          <button className={`seat${taken.has(seat)?' taken':''}${selected.includes(seat)?' mine':''}`} disabled={taken.has(seat)||!onChange} title={seat} onClick={()=>toggle(seat)}>{selected.includes(seat)?selected.indexOf(seat)+1:''}</button>}
          {L.aisleAfter.includes(c)&&<span className="aisle"/>}</React.Fragment>})}</div>)}
    <div className="legend"><span><i className="seat-sw"/>free</span><span><i className="seat-sw taken"/>taken</span>{onChange&&<span><i className="seat-sw mine"/>yours (passenger number)</span>}</div>
  </div>;
}

/** Passenger names + the price of every leg; books all legs in one request. */
function BookModal({legs,passengers,cabin,onClose,onBooked,onMessage}:{legs:{it:any,direction:string}[],passengers:number,cabin:string,onClose:()=>void,onBooked:(x:any)=>void,onMessage:(m:string)=>void}){
  const [pax,setPax]=useState(()=>Array.from({length:passengers},()=>({firstName:'',lastName:'',passport:''})));
  const [busy,setBusy]=useState(false);
  // One fare class for the whole trip: its price is the sum over both directions, on sale only if every flight has room.
  const fares=legs[0].it.fares.map((f:any)=>{const all=legs.map(x=>x.it.fares.find((g:any)=>g.code===f.code));
    return {...f,perPassenger:all.reduce((a:number,g:any)=>a+g.perPassenger,0),available:all.every((g:any)=>g.available),seatsAtFare:Math.min(...all.map((g:any)=>g.seatsAtFare))}});
  const [fare,setFare]=useState(()=>[...fares].filter((f:any)=>f.available).sort((a:any,b:any)=>a.perPassenger-b.perPassenger)[0]?.code||'FLEX');
  const chosen=fares.find((f:any)=>f.code===fare)||fares[0];
  const flights=legs.flatMap(x=>x.it.legs.map((l:any)=>({...l,direction:x.direction})));
  const [seats,setSeats]=useState<Record<string,string[]>>({}); const [maps,setMaps]=useState<Record<string,any>>({}); const [open,setOpen]=useState('');
  const key=(l:any)=>`${l.cabinId}@${l.date}`;
  async function openMap(l:any){const k=key(l);if(open===k){setOpen('');return}setOpen(k);
    try{setMaps({...maps,[k]:await api(`/flights/seatmap?cabinId=${l.cabinId}&date=${l.date}`)})}catch(e:any){onMessage(e.message)}}
  const perPax=chosen.perPassenger;
  const put=(i:number,k:string,v:string)=>setPax(pax.map((p,j)=>j===i?{...p,[k]:v}:p));
  async function book(){setBusy(true);try{
    const x=await api('/flight-bookings',{method:'POST',body:JSON.stringify({fareClass:fare,legs:flights.map((l:any)=>({cabinId:l.cabinId,date:l.date,direction:l.direction,seats:seats[key(l)]||[]})),passengers:pax})});
    onBooked(x)}catch(e:any){onMessage(e.message);setMaps({});setOpen('')}finally{setBusy(false)}}
  return <div className="modal" onClick={onClose}><div className="card wide" onClick={e=>e.stopPropagation()}><button onClick={onClose}>Close</button>
    <h2>Book {legs.length>1?'round trip':'one way'} · {CABIN_LABEL[cabin]} · {passengers} passenger{passengers===1?'':'s'}</h2>
    {legs.map(x=><div key={x.direction} className="book-part"><h4>{x.direction==='RETURN'?'Return':'Outbound'} · {fmtShort(x.it.legs[0].date)}</h4>
      {x.it.legs.map((l:any)=>{const k=key(l);return <div key={k}><LegLine l={l}/>
        <div className="row between"><div className="nightly">{x.it.fareClass===fare?<><b>{money(l.price)}</b> per passenger{l.parts.map((p:any)=><span key={p.layer} className={`ptag ${p.layer}`}>{p.name} {p.change}</span>)}</>:<small>price in {FARE_TAG[fare]} shown in the total below</small>}</div>
          <button onClick={()=>openMap(l)}>{seats[k]?.length?`Seats: ${seats[k].join(', ')}`:'Choose seats'}</button></div>
        {open===k&&maps[k]&&<><SeatMap map={maps[k]} selected={seats[k]||[]} max={passengers} onChange={v=>setSeats({...seats,[k]:v})}/>
          <small>Pick up to {passengers} seat{passengers===1?'':'s'} (in passenger order); passengers without a seat get the first free ones.</small></>}
      </div>})}</div>)}
    <h3>Fare</h3>
    <div className="fares">{fares.map((f:any)=><button key={f.code} className={`fare${fare===f.code?' active':''}`} disabled={!f.available} onClick={()=>setFare(f.code)}>
      <b>{f.name}</b><span className="price-big">{f.available?money(f.perPassenger):'Sold out'}</span><small>{refundText(f.refundPct)}{f.changeable?' · changes allowed':''}</small>
      {f.available&&f.seatsAtFare<20&&<small className="notice">{f.seatsAtFare} left at this fare</small>}</button>)}</div>
    <h3>Passengers</h3>
    {pax.map((p,i)=><div className="row" key={i}><b className="paxno">{i+1}</b>
      <input placeholder="First name" value={p.firstName} onChange={e=>put(i,'firstName',e.target.value)}/>
      <input placeholder="Last name" value={p.lastName} onChange={e=>put(i,'lastName',e.target.value)}/>
      <input placeholder="Passport (optional)" value={p.passport} onChange={e=>put(i,'passport',e.target.value)}/></div>)}
    <div className="row between"><div className="price-big">{money(perPax*passengers)}<small> total · {money(perPax)} per passenger{charged(perPax*passengers)}</small></div>
      <button className="pay" disabled={busy||pax.some(p=>!p.firstName.trim()||!p.lastName.trim())} onClick={book}>{busy?'Holding seats…':'Hold seats and continue to payment'}</button></div>
    <small>Seats on every flight are held for you at once (all or nothing) for 60 seconds; pay in My bookings to confirm. Cheaper fares sell out first: Saver closes when 40% of a cabin is sold, Standard at 85%.</small>
  </div></div>;
}

/** The user's last 10 searches (kept in Redis per account). Clicking one restores every field; past dates move to today. */
function RecentSearches({items,airports,onPick,onClear}:{items:any[],airports:any[],onPick:(h:any)=>void,onClear:()=>void}){
  if(!items.length) return null;
  const city=(iata:string)=>airports.find(a=>a.iata===iata)?.city||iata;
  return <section className="recent"><div className="row between"><b>Recent searches</b><button className="link" onClick={onClear}>Clear</button></div>
    <div className="recent-list">{items.map(h=>{const past=h.date<today;
      return <button key={h.key} className={past?'past':''} onClick={()=>onPick(h)} title={past?'These dates have passed: the trip moves to today, same length':`Searched ${new Date(h.searchedAt).toLocaleString()}`}>
        <b>{city(h.from)} {h.tripType==='ROUND_TRIP'?'⇄':'→'} {city(h.to)}</b>
        <small>{fmtShort(h.date)}{h.returnDate?` – ${fmtShort(h.returnDate)} (${tripDays(h.date,h.returnDate)})`:''} · {h.passengers} pax · {CABIN_LABEL[h.cabin]}{h.stops==='0'?' · direct':''}</small>
        <small>{past?'dates passed':h.cheapest?`from ${money(h.cheapest)} then`:`${h.results} options`}</small>
      </button>})}</div></section>;
}

function App(){
  const [user,setUser]=useState<any>(JSON.parse(localStorage.getItem('user')||'null'));
  const [email,setEmail]=useState('customer@example.com'); const [password,setPassword]=useState('customer123');
  const [stats,setStats]=useState<any>(null); const [airports,setAirports]=useState<any[]>([]);
  const [from,setFrom]=useState('TLV'); const [to,setTo]=useState('BKK');
  const [tripType,setTripType]=useState<'ONE_WAY'|'ROUND_TRIP'>('ROUND_TRIP');
  const [date,setDate]=useState(plusDays(today,14)); const [returnDate,setReturnDate]=useState(plusDays(today,28));
  const [passengers,setPassengers]=useState(1); const [cabin,setCabin]=useState('ECONOMY'); const [stops,setStops]=useState('1'); const [sort,setSort]=useState('price');
  const [results,setResults]=useState<any>(null); const [returnResults,setReturnResults]=useState<any>(null);
  const [outPick,setOutPick]=useState<any>(null); const [retPick,setRetPick]=useState<any>(null); const [booking,setBooking]=useState(false);
  const [refreshKey,setRefreshKey]=useState(0);
  const [history,setHistory]=useState<any[]>([]);
  const [view,setView]=useState<'search'|'bookings'|'myflights'|'dashboard'|'redis'|'redislog'|'pricing'|'flightdata'>(user?.role==='SELLER'?'dashboard':user?.role==='ADMIN'?'flightdata':'search');
  const [dashDays,setDashDays]=useState(7);
  const [myFlights,setMyFlights]=useState<any[]>([]); const [flightBookings,setFlightBookings]=useState<any>(null); const [pricesAirline,setPricesAirline]=useState<any>(null);
  const [bookings,setBookings]=useState<any[]>([]);
  const [customers,setCustomers]=useState<any[]>([]); const [customerId,setCustomerId]=useState(''); const [allCustomerCount,setAllCustomerCount]=useState(0);
  const [adminFlights,setAdminFlights]=useState<any[]>([]); const [raceCabin,setRaceCabin]=useState(''); const [raceDate,setRaceDate]=useState(plusDays(today,7)); const [racePax,setRacePax]=useState(2); const [raceSeat,setRaceSeat]=useState('');
  const [confirmNow,setConfirmNow]=useState(false); const [race,setRace]=useState<any>(null);
  const [currencies,setCurrencies]=useState<Currency[]>([THB]);
  const [currency,setCurrencyState]=useState(()=>{try{return localStorage.getItem('currency')||'THB'}catch{return 'THB'}});
  function setCurrency(c:string){setCurrencyState(c);try{localStorage.setItem('currency',c)}catch{}}
  async function loadCurrencies(){try{setCurrencies(await api('/currencies'))}catch{}}
  CUR=currencies.find(c=>c.code===currency)||THB;
  const [message,setMessage]=useState('');
  const [,setTick]=useState(0); // re-renders once a second while a payment countdown is running
  const [accounts,setAccounts]=useState<any[]>(ACCOUNTS); const [accountFilter,setAccountFilter]=useState(''); const [roleCounts,setRoleCounts]=useState<Record<string,number>>({});
  useEffect(()=>{if(user)return;
    api('/auth/demo-accounts').then((x:any)=>{if(x.accounts?.length){setAccounts(x.accounts.map((r:any)=>({label:r.name,email:r.email,password:r.password,role:r.role})));setRoleCounts(x.counts)}}).catch(()=>{});
    api('/stats').then(setStats).catch(()=>{})},[user]);

  const roundTrip=tripType==='ROUND_TRIP';
  const datesOk=date>=today&&(!roundTrip||returnDate>=date);
  async function login(e=email,p=password){try{const x=await api('/auth/login',{method:'POST',body:JSON.stringify({email:e,password:p})});localStorage.setItem('token',x.token);localStorage.setItem('user',JSON.stringify(x.user));setUser(x.user);setView(x.user.role==='SELLER'?'dashboard':x.user.role==='ADMIN'?'flightdata':'search');setMessage('Logged in')}catch(err:any){setMessage(err.message+(e.includes('example.com')&&e!=='admin@example.com'?' (login as Admin and Generate Sample Data first)':''))}}
  function quickLogin(a:{email:string,password:string}){setEmail(a.email);setPassword(a.password);login(a.email,a.password)}
  async function loadAirports(){try{setAirports(await api('/airports'))}catch(e:any){setMessage(e.message)}}
  const q=(f:string,t:string,d:string)=>`/flights/search?from=${f}&to=${t}&date=${d}&passengers=${passengers}&cabin=${cabin}&stops=${stops}&sort=${sort}&limit=50`;
  async function search(){if(!datesOk){setMessage('Choose valid dates');return}setOutPick(null);setRetPick(null);
    try{const [a,b]=await Promise.all([api(q(from,to,date)),roundTrip?api(q(to,from,returnDate)):Promise.resolve(null)]);setResults(a);setReturnResults(b);setRefreshKey(k=>k+1)}
    catch(e:any){setMessage(e.message)}}
  async function loadBookings(){try{setBookings(await api('/flight-bookings/me'))}catch(e:any){setMessage(e.message)}}
  async function pay(id:string){try{await api(`/flight-bookings/${id}/pay`,{method:'POST'});setMessage('Payment received. Booking confirmed.');loadBookings()}catch(e:any){setMessage(e.message);loadBookings()}}
  async function cancel(id:string){if(!confirm('Cancel this trip?'))return;try{const x=await api(`/flight-bookings/${id}/cancel`,{method:'POST'});setMessage(`Trip cancelled, seats released. ${FARE_TAG[x.fareClass]||''} fare: ${refundText(x.refundPct).toLowerCase()}, ${money(x.refundAmount||0)} back${x.refundAmount?charged(x.refundAmount):''}.`);loadBookings()}catch(e:any){setMessage(e.message)}}
  async function loadMyFlights(){try{setMyFlights(await api('/seller/flights'))}catch(e:any){setMessage(e.message)}}
  async function openFlightBookings(f:any){try{setFlightBookings({flight:f,rows:await api(`/seller/flights/${f.id}/bookings`)})}catch(e:any){setMessage(e.message)}}
  async function loadCustomers(){try{const all=(await api('/admin/users')).filter((u:any)=>u.role==='CUSTOMER');setAllCustomerCount(all.length);const us=all.filter((u:any)=>!u.email.startsWith('sim-'));setCustomers(us);if(!us.find((u:any)=>u.id===customerId))setCustomerId(us[0]?.id||'')}catch(e:any){setMessage(e.message)}}
  async function loadAdminFlights(){try{const fl=await api('/admin/flights');setAdminFlights(fl);const cabs=fl.flatMap((f:any)=>f.cabins);if(!cabs.find((c:any)=>c.id===raceCabin))setRaceCabin(cabs[0]?.id||'')}catch(e:any){setMessage(e.message)}}
  async function adminRefresh(){loadCustomers();loadAdminFlights();loadAirports()}
  async function sample(){try{setMessage((await api('/admin/sample-data',{method:'POST'})).message);adminRefresh()}catch(e:any){setMessage(e.message)}}
  async function delSample(){if(!confirm('Delete sample users, their bookings and synthetic flights? Real (aviationstack) flights are kept.'))return;try{setMessage((await api('/admin/sample-data',{method:'DELETE'})).message);adminRefresh();setRace(null)}catch(e:any){setMessage(e.message)}}
  async function sampleBookings(){if(!customerId){setMessage('Select a customer first');return}try{setMessage((await api('/admin/sample-bookings',{method:'POST',body:JSON.stringify({userId:customerId})})).message)}catch(e:any){setMessage(e.message)}}
  async function raceBooking(){try{const x=await api('/admin/concurrent-booking',{method:'POST',body:JSON.stringify({cabinId:raceCabin,date:raceDate,passengers:racePax,seat:raceSeat||undefined,confirm:confirmNow})});setRace(x);setMessage(x.message)}catch(e:any){setMessage(e.message)}}
  async function loadHistory(){try{setHistory(await api('/me/search-history'))}catch{}}
  async function clearHistory(){try{await api('/me/search-history',{method:'DELETE'});setHistory([])}catch(e:any){setMessage(e.message)}}
  function restoreSearch(h:any){
    const len=h.returnDate?Math.max(0,Math.round((Date.parse(h.returnDate)-Date.parse(h.date))/86400000)):14;
    const d=h.date<today?today:h.date;
    setFrom(h.from);setTo(h.to);setTripType(h.tripType);setDate(d);setReturnDate(h.returnDate&&h.date>=today?h.returnDate:plusDays(d,len));
    setPassengers(h.passengers);setCabin(h.cabin);setStops(h.stops);window.scrollTo({top:0,behavior:'smooth'})}
  function shiftDates(d:string){const len=Math.max(0,Math.round((Date.parse(returnDate)-Date.parse(date))/86400000));setDate(d);setReturnDate(plusDays(d,len))}

  useEffect(()=>{if(user){loadAirports();loadCurrencies();loadHistory()}},[user?.id]);
  // Record a search only when its results stay on screen for 2 seconds, so stepping through fields is not recorded.
  useEffect(()=>{if(!results||!user)return;
    const t=setTimeout(()=>{const ret=roundTrip?returnResults?.cheapest:0;
      api('/me/search-history',{method:'POST',body:JSON.stringify({from:results.from,to:results.to,tripType,date:results.date,returnDate:roundTrip?returnDate:null,passengers,cabin,stops,
        cheapest:results.cheapest!=null&&(!roundTrip||ret!=null)?results.cheapest+(ret||0):null,results:results.total})}).then(loadHistory).catch(()=>{})},2000);
    return()=>clearTimeout(t)},[results,returnResults]);
  useEffect(()=>{if(user&&airports.length&&datesOk)search()},[airports.length,from,to,date,returnDate,tripType,passengers,cabin,stops,sort]);
  useEffect(()=>{if(view==='bookings'&&user)loadBookings();if(view==='myflights'&&user)loadMyFlights()},[view]);
  useEffect(()=>{if(user?.role==='ADMIN')adminRefresh()},[user]);
  const pending=bookings.filter(b=>b.status==='PENDING');
  useEffect(()=>{ // countdown; when a hold runs out, reload so the server's PAYMENT_TIMEOUT status shows up
    if(view!=='bookings'||pending.length===0)return;
    const t=setInterval(()=>{setTick(x=>x+1);if(pending.some(b=>secondsLeft(b)===0))loadBookings()},1000);
    return()=>clearInterval(t)},[view,pending.map(b=>b.id).join()]);

  if(!user) return <main><h1>Flight Booking Lab <small>Tel Aviv ⇄ Bangkok</small></h1><div className="card"><h2>Login</h2>
    {/* Demo credentials only: the secret field is a masked text input (not type="password") so the browser's
        password manager does not offer to save it or warn that "admin123" appears in a data breach. */}
    <input value={email} onChange={e=>setEmail(e.target.value)} placeholder="email" name="demo-email" autoComplete="off" spellCheck={false} data-lpignore="true" data-1p-ignore/>
    <input value={password} onChange={e=>setPassword(e.target.value)} onKeyDown={e=>{if(e.key==='Enter')login()}} placeholder="password" name="demo-secret" className="secret" type="text" autoComplete="off" spellCheck={false} data-lpignore="true" data-1p-ignore/>
    <button onClick={()=>login()}>Login</button>
    <p className="message">{message}</p>
    {stats&&<div className="tiles">
      <Tile label="Flights" value={num(stats.flights)} hint={`${stats.realFlights} real (aviationstack) · ${stats.airlines} airlines · ${stats.airports} airports`}/>
      <Tile label="Market" value={stats.market}/>
      <Tile label="Seats bookable now" value={num(stats.availableSeats)} hint={`every flight × every day it flies (${fmt(stats.firstDay)} → ${fmt(stats.lastDay)}) − ${num(stats.bookedSeats)} booked`}/>
    </div>}
    {(()=>{const qf=accountFilter.trim().toLowerCase();const list=accounts.filter(a=>!qf||a.email.toLowerCase().includes(qf)||(a.label||'').toLowerCase().includes(qf));
      return <>
      <div className="row between"><h3>All accounts <small>· click one to log in</small></h3>
        <input value={accountFilter} onChange={e=>setAccountFilter(e.target.value)} placeholder="Filter by email or name" autoComplete="off"/></div>
      <div className="account-tables">{ROLE_TABLES.map(([role,title])=>{const rows=list.filter(a=>a.role===role);const total=roleCounts[role]??accounts.filter(a=>a.role===role).length;
        return <section key={role}><h4><span className={`role ${role}`}>{title}</span> <small>{total.toLocaleString()}{qf?` · ${rows.length.toLocaleString()} match`:''}</small></h4>
        <div className="accounts-scroll"><table className="accounts"><thead><tr><th>Name</th><th>Email</th><th>Password</th></tr></thead><tbody>
          {rows.map(a=><tr key={a.email} onClick={()=>quickLogin(a)} title="Click to log in"><td>{a.label}</td><td>{a.email}</td><td><code>{a.password}</code></td></tr>)}
          {rows.length===0&&<tr><td colSpan={3}>{qf?`No ${title.toLowerCase()} match "${accountFilter}"`:`No ${title.toLowerCase()} yet`}</td></tr>}
        </tbody></table></div></section>})}</div></>})()}
    <p><small>Only Admin exists on a fresh database. Log in as Admin, import real flights under "Flight data" (or skip it), then press "Generate Sample Data" to create airline sellers and customers; load simulations add customers named "Sim …" (password sim123).</small></p>
    <h3>Services</h3>
    <div className="tablewrap"><table className="services"><thead><tr><th>Service</th><th>Address</th><th>Used for</th></tr></thead><tbody>
      {SERVICES.map(sv=><tr key={sv.name}><td>{sv.name}</td><td>{sv.href?<a href={sv.value} target="_blank" rel="noreferrer">{sv.value}</a>:<code>{sv.value}</code>}</td><td><small>{sv.usedFor}</small></td></tr>)}
    </tbody></table></div>
    <p><small>Peek at Redis: <code>podman exec -it flight-booking-lab_redis_1 redis-cli --scan --pattern {"'fs:{*'"}</code> (one hash per cabin and month with bookings; <code>HGETALL</code> one to see booked seats per day)</small></p>
    <p><small>Kafka topics to watch in Kafka UI: flight.booking.created, flight.booking.confirmed, flight.booking.payment_timeout, flight.booking.cancelled, flight.seats.changed, flight.data.imported.</small></p>
  </div></main>;

  const results2=roundTrip?returnResults:null;
  const canBook=user.role==='CUSTOMER'&&outPick&&(!roundTrip||retPick);
  return <main>
    <header>
      <nav>
        <button className={view==='search'?'active':''} onClick={()=>setView('search')}>Search</button>
        {user.role==='CUSTOMER'&&<button className={view==='bookings'?'active':''} onClick={()=>setView('bookings')}>My bookings</button>}
        {user.role==='SELLER'&&<button className={view==='dashboard'?'active':''} onClick={()=>setView('dashboard')}>Dashboard</button>}
        {user.role==='SELLER'&&<button className={view==='myflights'?'active':''} onClick={()=>setView('myflights')}>My flights</button>}
        {user.role==='ADMIN'&&<button className={view==='flightdata'?'active':''} onClick={()=>setView('flightdata')}>Flight data</button>}
        {user.role==='ADMIN'&&<button className={view==='redis'?'active':''} onClick={()=>setView('redis')}>Redis records</button>}
        {user.role==='ADMIN'&&<button className={view==='redislog'?'active':''} onClick={()=>setView('redislog')}>Availability log</button>}
        {user.role==='ADMIN'&&<button className={view==='pricing'?'active':''} onClick={()=>setView('pricing')}>Country pricing</button>}
        <button onClick={()=>{localStorage.clear();location.reload()}}>Logout</button>
      </nav>
      <h1>Flight Booking Lab</h1>
      <span className="row"><label className="inline" title="Display only: prices are charged in baht, converted at fixed rates">Currency<select value={CUR.code} onChange={e=>setCurrency(e.target.value)}>
        {currencies.map(c=><option key={c.code} value={c.code}>{c.symbol} {c.code}</option>)}</select></label><span>{user.name} · {user.role}</span></span>
    </header>
    <p className="message">{message}</p>
    {view==='flightdata'&&user.role==='ADMIN'&&<FlightData onMessage={setMessage} onImported={adminRefresh}/>}
    {view==='redis'&&user.role==='ADMIN'&&<RedisRecords onMessage={setMessage}/>}
    {view==='redislog'&&user.role==='ADMIN'&&<AvailabilityLog onMessage={setMessage}/>}
    {view==='pricing'&&user.role==='ADMIN'&&<><CurrencyRates currencies={currencies} onChange={loadCurrencies} onMessage={setMessage}/><CountryPricing onMessage={setMessage}/></>}
    {user.role==='ADMIN'&&view==='search'&&<section className="card admin"><h2>Admin</h2>
      <div className="row"><button onClick={sample}>Generate Sample Data</button><button onClick={delSample}>Delete Sample Data</button>
        <button onClick={async()=>{try{setMessage((await api('/admin/rebuild-availability',{method:'POST'})).message)}catch(e:any){setMessage(e.message)}}} title="Recompute Redis booked-seat counters from PostgreSQL bookings (also runs at API startup)">Rebuild Redis availability</button></div>
      <small>Sample data never calls aviationstack: it uses the flights already imported (Flight data tab), or synthetic TLV ⇄ BKK flights if there are none.</small>
      <div className="row">
        <select value={customerId} onChange={e=>setCustomerId(e.target.value)} disabled={!customers.length}>
          {customers.length===0&&<option value="">No customers yet</option>}
          {customers.map(c=><option key={c.id} value={c.id}>{c.name} ({c.email})</option>)}
        </select>
        <button onClick={sampleBookings} disabled={!customerId}>Create Sample Bookings for selected customer</button>
      </div>
      <small>Creates a direct round trip for 2, a one-way with a connection, and two trips on the same day (so the double-booking notice shows) for that customer, all confirmed.</small>
      <h3>Concurrency demo: all customers book the same departure at the same time</h3>
      <div className="row">
        <select value={raceCabin} onChange={e=>setRaceCabin(e.target.value)} disabled={!adminFlights.length}>
          {adminFlights.length===0&&<option value="">No flights yet</option>}
          {adminFlights.flatMap((f:any)=>f.cabins.map((c:any)=><option key={c.id} value={c.id}>{f.flightNumber} {f.from} → {f.to} {f.depLocal} · {CABIN_LABEL[c.cabin]} · {c.totalSeats} seats</option>))}
        </select>
        <label>Date<input type="date" min={today} value={raceDate} onChange={e=>setRaceDate(e.target.value)}/></label>
        <label>Seats per customer<input type="number" min={1} max={9} value={racePax} disabled={!!raceSeat} onChange={e=>setRacePax(Number(e.target.value))}/></label>
        <label>Or everyone wants seat<input value={raceSeat} placeholder="e.g. 1A" onChange={e=>setRaceSeat(e.target.value.toUpperCase())} style={{width:90}}/></label>
        <label className="check"><input type="checkbox" checked={confirmNow} onChange={e=>setConfirmNow(e.target.checked)}/>Confirm immediately (skip the 60s payment window)</label>
        <button onClick={raceBooking} disabled={!raceCabin||!allCustomerCount}>Book with all {allCustomerCount} customers at once</button>
      </div>
      <small>Every customer (including simulation customers, if any) books the same departure in the same instant. The atomic Redis script decides who gets seats; the others are rejected. Business cabins are small: use them (or many simulation customers) to see a sell-out.</small>
      {race&&<div className="tablewrap"><table><thead><tr><th>Customer</th><th>Result</th><th>Seats left after</th></tr></thead><tbody>
        {race.results.map((x:any)=><tr key={x.email} className={x.ok?'':'warn'}><td>{x.customer}<br/><small>{x.email}</small></td><td>{x.ok?<span className={`status ${x.status}`}>{STATUS_LABEL[x.status]}</span>:<span className="notice">✕ {x.error}</span>}</td><td>{x.ok?<>{x.seatsLeft}{x.seats?<small> · seat {x.seats.join(', ')}</small>:null}</>:'—'}</td></tr>)}
      </tbody></table><small>{race.flight} · {CABIN_LABEL[race.cabin]} · {fmt(race.date)}{race.seat?` · seat ${race.seat}`:''} · seats free {race.seatsBefore} → {race.seatsAfter} · {race.durationMs} ms</small></div>}
      <SimulationPanel onMessage={setMessage} flights={adminFlights}/>
    </section>}

    {user.role==='SELLER'&&view==='search'&&<p><small>As a seller, search only shows your own airlines' flights.</small></p>}
    {view==='dashboard'&&user.role==='SELLER'&&<SellerDashboard days={dashDays} setDays={setDashDays} onMessage={setMessage}/>}
    {view==='myflights'&&<section className="card">
      <h2>My flights</h2>
      {myFlights.length===0?<p>Your airlines have no flights yet.</p>:<>
      <div className="row">{[...new Map(myFlights.map(f=>[f.airlineIata,{iata:f.airlineIata,name:f.airline}])).values()].map(a=><button key={a.iata} onClick={()=>setPricesAirline(a)}>Prices · {a.name}</button>)}</div>
      <div className="tablewrap"><table><thead><tr><th>Flight</th><th>Route</th><th>Departs</th><th>Arrives</th><th>Duration</th><th>Cabins</th><th>Upcoming bookings</th><th></th></tr></thead><tbody>
        {myFlights.map(f=><tr key={f.id}>
          <td><b>{f.flightNumber}</b><br/><small>{f.airline}{f.source==='SAMPLE'?' · sample':''}</small></td><td>{f.fromCity} ({f.from}) → {f.toCity} ({f.to})</td><td>{f.depLocal}</td><td>{f.arrLocal}{f.arrDayOffset>0&&<sup>+{f.arrDayOffset}</sup>}</td><td>{dur(f.durationMin)}</td>
          <td>{(f.cabins||[]).map((c:any)=><div key={c.id}><small>{CABIN_LABEL[c.cabin]} · {c.totalSeats} seats · {money(c.price)}</small></div>)}</td><td>{f.upcomingBookings}</td>
          <td className="actions"><button onClick={()=>openFlightBookings(f)}>Bookings</button></td>
        </tr>)}
      </tbody></table></div></>}
    </section>}

    {pricesAirline&&<PricesModal airline={pricesAirline} onClose={()=>setPricesAirline(null)} onMessage={setMessage}/>}
    {flightBookings&&<div className="modal" onClick={()=>setFlightBookings(null)}><div className="card wide" onClick={e=>e.stopPropagation()}><button onClick={()=>setFlightBookings(null)}>Close</button>
      <h2>Bookings · {flightBookings.flight.flightNumber} {flightBookings.flight.from} → {flightBookings.flight.to}</h2>
      {flightBookings.rows.length===0?<p>No bookings on this flight.</p>:
      <div className="tablewrap"><table><thead><tr><th>Customer</th><th>Trip</th><th>Passengers</th><th>Total</th><th>Status</th></tr></thead><tbody>
        {flightBookings.rows.map((b:any)=><tr key={b.id}><td>{b.customerName}<br/><small>{b.customerEmail}</small></td>
          <td>{b.legs.map((l:any)=><div key={l.seq}><small>{l.flightNumber===flightBookings.flight.flightNumber?<b>{l.flightNumber}</b>:l.flightNumber} {l.from} → {l.to} · {fmt(l.date)} · {CABIN_LABEL[l.cabin]} {FARE_TAG[l.fareClass]||''}{l.seats?.length?` · ${l.seats.map((x:any)=>x.seat).join(', ')}`:''}</small></div>)}</td>
          <td>{b.passengers}<br/><small>{(b.passengerList||[]).map((p:any)=>`${p.firstName} ${p.lastName}`).join(', ')}</small></td><td>{money(b.price)}</td><td><span className={`status ${b.status}`}>{STATUS_LABEL[b.status]||b.status}</span></td></tr>)}
      </tbody></table></div>}
    </div></div>}

    {view==='search'&&<>
      <section className="search">
        <AirportSelect label="From" airports={airports} value={from} onChange={v=>{setFrom(v);if(v===to)setTo(from)}}/>
        <button className="swap" title="Swap" onClick={()=>{setFrom(to);setTo(from)}}>⇄</button>
        <AirportSelect label="To" airports={airports} value={to} onChange={v=>{setTo(v);if(v===from)setFrom(to)}}/>
        <span className="toggle"><button className={!roundTrip?'active':''} onClick={()=>setTripType('ONE_WAY')}>One way</button><button className={roundTrip?'active':''} onClick={()=>setTripType('ROUND_TRIP')}>Round trip</button></span>
        <label>Depart<input type="date" min={today} value={date} onChange={e=>{setDate(e.target.value);if(returnDate<e.target.value)setReturnDate(e.target.value)}}/></label>
        {roundTrip&&<label>Return<input type="date" min={date} value={returnDate} onChange={e=>setReturnDate(e.target.value)}/></label>}
        {roundTrip&&<span className="nights" title="Calendar days between departure and return">{datesOk?tripDays(date,returnDate):'Invalid dates'}</span>}
        <label>Passengers<select value={passengers} onChange={e=>setPassengers(Number(e.target.value))}>{[1,2,3,4,5,6,7,8,9].map(n=><option key={n} value={n}>{n}</option>)}</select></label>
        <label>Cabin<select value={cabin} onChange={e=>setCabin(e.target.value)}><option value="ECONOMY">Economy</option><option value="BUSINESS">Business</option></select></label>
        <label>Stops<select value={stops} onChange={e=>setStops(e.target.value)}><option value="1">Direct or 1 stop</option><option value="0">Direct only</option></select></label>
        <label>Sort<select value={sort} onChange={e=>setSort(e.target.value)}><option value="price">Cheapest</option><option value="duration">Fastest</option><option value="departure">Earliest</option></select></label>
        {/* Move the whole trip, keeping its length; never into the past. */}
        <span className="toggle shift">{SHIFTS.map(([label,n,unit])=>{const d=unit==='month'?plusMonths(date,n):plusDays(date,n);
          return <button key={label} disabled={d<today} title={`Depart ${fmt(d)}`} onClick={()=>shiftDates(d)}>{label}</button>})}</span>
      </section>
      <RecentSearches items={history} airports={airports} onPick={restoreSearch} onClear={clearHistory}/>
      <DateStrip from={from} to={to} date={date} cabin={cabin} passengers={passengers} onPick={shiftDates} refreshKey={refreshKey}/>
      {CUR.code!=='THB'&&<p><small>Prices shown in {CUR.code} at a fixed rate of ฿{CUR.thbPerUnit} per {CUR.symbol}1; you are charged in baht.</small></p>}
      {canBook&&<div className="bookbar card"><span><b>{roundTrip?`Round trip · ${tripDays(date,returnDate)}`:'One way'}</b> · {passengers} passenger{passengers===1?'':'s'} · {money((outPick.pricePerPassenger+(retPick?.pricePerPassenger||0))*passengers)}</span><button className="pay" onClick={()=>setBooking(true)}>Continue to booking</button></div>}
      {results&&<><h3>{roundTrip?'1 · Outbound':'Flights'} · {airports.find(a=>a.iata===from)?.city} → {airports.find(a=>a.iata===to)?.city} · {fmtShort(date)} <small>{results.total} option{results.total===1?'':'s'}</small></h3>
        {results.items.length===0?<p>No flights on this day. Try another date above.</p>:
        results.items.map((it:any)=><ItineraryCard key={it.id} it={it} passengers={passengers} picked={outPick?.id===it.id} disabled={user.role!=='CUSTOMER'}
          actionLabel={user.role!=='CUSTOMER'?'Customers only':outPick?.id===it.id?'✓ Selected':roundTrip?'Select outbound':'Select'} onPick={()=>setOutPick(it)}/>)}</>}
      {results2&&<><h3>2 · Return · {airports.find(a=>a.iata===to)?.city} → {airports.find(a=>a.iata===from)?.city} · {fmtShort(returnDate)} <small>{results2.total} option{results2.total===1?'':'s'}</small></h3>
        <DateStrip from={to} to={from} date={returnDate} minDate={date} cabin={cabin} passengers={passengers} onPick={setReturnDate} refreshKey={refreshKey}/>
        {results2.items.length===0?<p>No return flights on this day.</p>:
        results2.items.map((it:any)=>{const tooEarly=outPick&&Date.parse(it.depAt)<Date.parse(outPick.arrAt);
          return <ItineraryCard key={it.id} it={it} passengers={passengers} picked={retPick?.id===it.id} disabled={user.role!=='CUSTOMER'||tooEarly}
            actionLabel={tooEarly?'Leaves before you land':retPick?.id===it.id?'✓ Selected':'Select return'} onPick={()=>setRetPick(it)}/>})}</>}
    </>}
    {booking&&canBook&&<BookModal legs={[{it:outPick,direction:'OUTBOUND'},...(roundTrip?[{it:retPick,direction:'RETURN'}]:[])]} passengers={passengers} cabin={cabin} onMessage={setMessage}
      onClose={()=>setBooking(false)} onBooked={x=>{setBooking(false);setMessage(`Seats held for you for ${x.paymentWindowSeconds}s: ${money(x.totalPrice)} for ${x.passengers} passenger(s). Press Pay in My bookings to confirm.`);setView('bookings');loadBookings()}}/>}

    {view==='bookings'&&<section className="card">
      <h2>My bookings</h2>
      {pending.length>0&&<p className="lockinfo">You have {pending.length} trip{pending.length===1?'':'s'} with seats held. Pay before the countdown ends or the seats are released to other customers.</p>}
      {bookings.length===0?<p>No bookings yet.</p>:
      <div className="tablewrap"><table className="bookings"><thead><tr><th>Trip</th><th>Flights</th><th>Passengers</th><th>Total</th><th>Status</th><th>Note</th><th></th></tr></thead><tbody>
        {bookings.map(b=>{const first=b.legs[0],last=b.legs[b.legs.length-1],out=b.legs.filter((l:any)=>l.direction==='OUTBOUND');
          return <tr key={b.id} className={b.status==='PENDING'?'pending':b.overlaps?.length?'warn':''}>
          <td><b>{first.fromCity} → {out[out.length-1].toCity}</b><br/><small>{b.tripType==='ROUND_TRIP'?`Round trip, back ${fmt(last.arrAt)}`:'One way'} · {CABIN_LABEL[first.cabin]}</small></td>
          <td>{b.legs.map((l:any)=><div key={l.seq}><small>{l.direction==='RETURN'&&l.seq===out.length+1?'↩ ':''}<b>{l.flightNumber}</b> {l.from} {l.depLocal} → {l.to} {l.arrLocal}{l.arrDayOffset>0?`+${l.arrDayOffset}`:''} · {fmtShort(l.date)}{l.seats?.length?<> · seat{l.seats.length>1?'s':''} <b title={l.seats.map((x:any)=>`${x.seat} ${x.passenger}`).join('\n')}>{l.seats.map((x:any)=>x.seat).join(', ')}</b></>:null}</small></div>)}</td>
          <td>{b.passengers}<br/><small>{(b.passengerList||[]).map((p:any)=>`${p.firstName} ${p.lastName}`).join(', ')}</small></td>
          <td title={(b.priceBreakdown||[]).map((p:any)=>`${p.flightNumber} ${fmt(p.date)}: ${money(p.price)}${p.label?` (${p.label})`:''}`).join('\n')}>{money(b.price)}<br/><small>{money(b.price/b.passengers)} / passenger</small></td>
          <td><span className={`status ${b.status}`}>{STATUS_LABEL[b.status]||b.status}</span>{b.status==='PENDING'&&<><br/><span className="countdown">{secondsLeft(b)}s left</span></>}
            <br/><small className={`fare-tag ${b.fareClass}`}>{FARE_TAG[b.fareClass]||b.fareClass}</small>{b.status==='CANCELLED'&&b.refundAmount!=null&&<><br/><small>Refund {money(b.refundAmount)}</small></>}</td>
          <td>{b.overlaps?.length>0&&<span className="notice" title={b.overlaps.map((o:any)=>`${o.route} ${fmt(o.depAt)}`).join('\n')}>⚠ Double booking: overlaps {b.overlaps.map((o:any)=>o.route).join(', ')}</span>}</td>
          <td className="actions">
            {(()=>{const ph=tripPhase(b);return ph&&<span className={`phase ${ph.key}`} title={ph.hint}><b>{ph.label}</b><br/><small>{ph.hint}</small></span>})()}
            {b.status==='PENDING'&&<button className="pay" onClick={()=>pay(b.id)} disabled={secondsLeft(b)===0} title={`Charged in baht: ฿${Math.round(Number(b.price)).toLocaleString()}`}>Pay {money(b.price)}</button>}
            {(b.status==='CONFIRMED'||b.status==='PENDING')&&Date.parse(first.depAt)>Date.now()&&<button className="danger" onClick={()=>cancel(b.id)}>Cancel</button>}
          </td>
        </tr>})}
      </tbody></table></div>}
    </section>}
  </main>
}
createRoot(document.getElementById('root')!).render(<App/>);
