const ENDPOINTS = {
  keyword: "https://openapi.rakuten.co.jp/engine/api/Travel/KeywordHotelSearch/20170426",
  simple: "https://openapi.rakuten.co.jp/engine/api/Travel/SimpleHotelSearch/20170426",
  vacant: "https://openapi.rakuten.co.jp/engine/api/Travel/VacantHotelSearch/20170426",
  area: "https://openapi.rakuten.co.jp/engine/api/Travel/GetAreaClass/20140210",
};

const $ = (id) => document.getElementById(id);

const ui = {
  appId: $("appId"),
  accessKey: $("accessKey"),
  affiliateId: $("affiliateId"),
  fileInput: $("fileInput"),
  idCol: $("idCol"),
  nameCol: $("nameCol"),
  maxRows: $("maxRows"),
  nameConcurrency: $("nameConcurrency"),
  rps: $("rps"),
  auditTopN: $("auditTopN"),
  maxRetries: $("maxRetries"),
  fetchVacant: $("fetchVacant"),
  checkinDate: $("checkinDate"),
  checkoutDate: $("checkoutDate"),
  adultNum: $("adultNum"),
  roomNum: $("roomNum"),
  runBtn: $("runBtn"),
  clearLogBtn: $("clearLogBtn"),
  log: $("log"),
  downloads: $("downloads"),
  summary: $("summary"),
  statusBadges: $("statusBadges"),
};

initDefaultDates();
bindEvents();

function bindEvents() {
  ui.clearLogBtn.addEventListener("click", () => {
    ui.log.textContent = "";
  });

  ui.runBtn.addEventListener("click", async () => {
    try {
      ui.runBtn.disabled = true;
      ui.runBtn.textContent = "実行中...";
      await runPipeline();
    } catch (error) {
      log(error?.message || String(error), "error");
      alert(error?.message || String(error));
    } finally {
      ui.runBtn.disabled = false;
      ui.runBtn.textContent = "実行する";
    }
  });
}

function initDefaultDates() {
  const now = new Date();
  const checkin = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 30);
  const checkout = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 31);
  ui.checkinDate.value = formatDate(checkin);
  ui.checkoutDate.value = formatDate(checkout);
}

function formatDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function log(message, kind = "info") {
  const ts = new Date().toLocaleTimeString();
  const line = document.createElement("div");
  line.textContent = `[${ts}] ${message}`;
  if (kind === "error") line.className = "log-line--error";
  if (kind === "ok") line.className = "log-line--ok";
  ui.log.appendChild(line);
  ui.log.scrollTop = ui.log.scrollHeight;
  console[kind === "error" ? "error" : "log"](message);
}

function setBadges(items) {
  ui.statusBadges.innerHTML = "";
  items.forEach((text) => {
    const badge = document.createElement("span");
    badge.className = "badge";
    badge.textContent = text;
    ui.statusBadges.appendChild(badge);
  });
}

function setSummary(obj) {
  if (!obj || Object.keys(obj).length === 0) {
    ui.summary.innerHTML = "";
    return;
  }

  ui.summary.innerHTML = `
    <table class="summary-table">
      <tbody>
        ${Object.entries(obj)
          .map(
            ([key, value]) => `
              <tr>
                <th>${escapeHtml(key)}</th>
                <td>${escapeHtml(String(value))}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function clearDownloads() {
  ui.downloads.innerHTML = "";
}

function createDownloadLink(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.className = "download-link";
  link.href = url;
  link.download = filename;
  link.textContent = filename;
  ui.downloads.appendChild(link);
}

function createJsonDownload(filename, data) {
  createDownloadLink(filename, JSON.stringify(data, null, 2), "application/json;charset=utf-8");
}

function createCsvDownload(filename, rows) {
  const csv = Papa.unparse(rows);
  createDownloadLink(filename, csv, "text/csv;charset=utf-8");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeName(name) {
  if (name == null) return "";
  let s = String(name).normalize("NFKC").trim().toLowerCase();
  s = s.replace(/\u3000/g, " ");
  s = s.replace(/\s+/g, "");
  s = s.replace(/[‐-‒–—―ーｰ_/・･.,，、。:：;；'"“”‘’`´~〜!！?？()\[\]{}（）【】<>＜＞]/g, "");
  return s;
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function flattenForCsv(record) {
  const output = {};
  for (const [key, value] of Object.entries(record)) {
    if (value == null) {
      output[key] = "";
    } else if (typeof value === "object") {
      output[key] = JSON.stringify(value);
    } else {
      output[key] = value;
    }
  }
  return output;
}

function chooseMatch(inputName, candidates) {
  const exact = candidates.filter(
    (candidate) => String(candidate.hotelName || "").trim() === String(inputName).trim()
  );
  if (exact.length === 1) return [exact[0], "exact"];
  if (exact.length > 1) return [exact[0], "exact_multiple"];

  const normalizedInput = normalizeName(inputName);
  const normalized = candidates.filter(
    (candidate) => normalizeName(String(candidate.hotelName || "")) === normalizedInput
  );
  if (normalized.length === 1) return [normalized[0], "normalized"];
  if (normalized.length > 1) return [normalized[0], "normalized_multiple"];

  return [null, null];
}

function summarizeCandidate(candidate) {
  const keys = [
    "hotelNo",
    "hotelName",
    "hotelKanaName",
    "address1",
    "address2",
    "postalCode",
    "nearestStation",
    "areaName",
    "middleClassCode",
    "smallClassCode",
    "reviewAverage",
    "reviewCount",
    "hotelInformationUrl",
  ];

  const out = {};
  for (const key of keys) {
    out[key] = candidate?.[key] ?? "";
  }
  return out;
}

function extractHotels(responseJson) {
  const hotels = Array.isArray(responseJson?.hotels) ? responseJson.hotels : [];
  const result = [];

  for (const entry of hotels) {
    if (!entry || typeof entry !== "object") continue;
    const merged = {};

    if ("hotel" in entry) {
      const inner = entry.hotel;
      if (Array.isArray(inner)) {
        for (const part of inner) {
          if (part && typeof part === "object") {
            for (const [key, value] of Object.entries(part)) {
              if (value && typeof value === "object" && !Array.isArray(value)) {
                Object.assign(merged, value);
              } else if (value !== undefined) {
                merged[key] = value;
              }
            }
          }
        }
      } else if (inner && typeof inner === "object") {
        for (const [key, value] of Object.entries(inner)) {
          if (value && typeof value === "object" && !Array.isArray(value)) {
            Object.assign(merged, value);
          } else if (value !== undefined) {
            merged[key] = value;
          }
        }
      }
    } else {
      for (const [key, value] of Object.entries(entry)) {
        if (value && typeof value === "object" && !Array.isArray(value)) {
          Object.assign(merged, value);
        } else {
          merged[key] = value;
        }
      }
    }

    if (Object.keys(merged).length > 0) {
      result.push(merged);
    }
  }

  return result;
}

function parseAreaRows(areaJson) {
  const rows = [];

  function walk(node, ctx) {
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, { ...ctx }));
      return;
    }

    if (!node || typeof node !== "object") return;

    const nextCtx = { ...ctx };

    for (const level of ["large", "middle", "small", "detail"]) {
      const codeKey = `${level}ClassCode`;
      const nameKey = `${level}ClassName`;
      if (codeKey in node) nextCtx[codeKey] = node[codeKey];
      if (nameKey in node) nextCtx[nameKey] = node[nameKey];
    }

    const hasAnyCode = [
      "largeClassCode",
      "middleClassCode",
      "smallClassCode",
      "detailClassCode",
    ].some((key) => key in nextCtx);

    if (
      hasAnyCode &&
      ("detailClassCode" in nextCtx ||
        ("smallClassCode" in nextCtx && !("detailClasses" in node) && !("detailClass" in node)))
    ) {
      rows.push({
        largeClassCode: nextCtx.largeClassCode ?? "",
        largeClassName: nextCtx.largeClassName ?? "",
        middleClassCode: nextCtx.middleClassCode ?? "",
        middleClassName: nextCtx.middleClassName ?? "",
        smallClassCode: nextCtx.smallClassCode ?? "",
        smallClassName: nextCtx.smallClassName ?? "",
        detailClassCode: nextCtx.detailClassCode ?? "",
        detailClassName: nextCtx.detailClassName ?? "",
      });
    }

    for (const value of Object.values(node)) {
      walk(value, nextCtx);
    }
  }

  walk(areaJson, {});

  const seen = new Set();
  return rows.filter((row) => {
    const key = JSON.stringify(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function joinByLookup(baseRows, keyField, lookup, prefix = "") {
  return baseRows.map((row) => {
    const key = String(row[keyField] ?? "");
    const extra = lookup.get(key) || {};
    const prefixed = {};
    for (const [k, v] of Object.entries(extra)) {
      prefixed[`${prefix}${k}`] = v;
    }
    return { ...row, ...prefixed };
  });
}

function pLimit(concurrency) {
  let activeCount = 0;
  const queue = [];

  const next = () => {
    if (activeCount >= concurrency || queue.length === 0) return;

    activeCount++;
    const { fn, resolve, reject } = queue.shift();

    fn()
      .then(resolve)
      .catch(reject)
      .finally(() => {
        activeCount--;
        next();
      });
  };

  return (fn) =>
    new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
}

class RateLimiter {
  constructor(rps) {
    this.interval = 1000 / Math.max(rps, 0.01);
    this.nextTime = 0;
    this.lock = Promise.resolve();
  }

  async wait() {
    let release;
    const previous = this.lock;
    this.lock = new Promise((resolve) => {
      release = resolve;
    });

    await previous;

    const now = Date.now();
    const target = Math.max(now, this.nextTime);
    const waitMs = Math.max(0, target - now);
    this.nextTime = target + this.interval;
    release();

    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }
}

class RakutenBrowserClient {
  constructor({ appId, accessKey, affiliateId, rps, maxRetries }) {
    this.appId = appId;
    this.accessKey = accessKey;
    this.affiliateId = affiliateId || "";
    this.maxRetries = maxRetries;
    this.rateLimiter = new RateLimiter(rps);
    this.cache = new Map();
  }

  buildQuery(params) {
    const query = new URLSearchParams();
    query.set("applicationId", this.appId);
    query.set("accessKey", this.accessKey);
    query.set("format", "json");
    query.set("formatVersion", "2");

    if (this.affiliateId) {
      query.set("affiliateId", this.affiliateId);
    }

    for (const [key, value] of Object.entries(params || {})) {
      if (value === null || value === undefined || value === "") continue;
      query.set(key, String(value));
    }

    return query;
  }

  async getJson(endpointName, url, params = {}) {
    const cacheKey = `${endpointName}::${JSON.stringify(params)}`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    const fullUrl = `${url}?${this.buildQuery(params).toString()}`;
    let lastError = null;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      await this.rateLimiter.wait();

      try {
        const response = await fetch(fullUrl, {
          method: "GET",
          mode: "cors",
          credentials: "omit",
          cache: "no-store",
        });

        const text = await response.text();
        let data;
        try {
          data = JSON.parse(text);
        } catch {
          data = { rawText: text };
        }

        if (!response.ok) {
          throw new Error(`[${endpointName}] HTTP ${response.status} ${JSON.stringify(data).slice(0, 1200)}`);
        }

        this.cache.set(cacheKey, data);
        return data;
      } catch (error) {
        lastError = error;
        if (attempt < this.maxRetries) {
          const waitMs = Math.min(8000, 700 * (attempt + 1));
          log(`再試行 ${attempt + 1}/${this.maxRetries}: ${error.message}`, "error");
          await sleep(waitMs);
        }
      }
    }

    throw lastError;
  }
}

async function parseInputFile(file) {
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      complete: (results) => {
        if (results.errors && results.errors.length) {
          reject(new Error(results.errors.map((e) => e.message).join(" | ")));
          return;
        }
        resolve(results.data || []);
      },
      error: reject,
    });
  });
}

async function processOneName(client, hotelName, auditTopN) {
  const response = await client.getJson("keyword_hotel_search", ENDPOINTS.keyword, {
    keyword: hotelName,
    searchField: 1,
    hits: 30,
    page: 1,
    responseType: "large",
  });

  const hotels = extractHotels(response);
  const [matched, matchRule] = chooseMatch(hotelName, hotels);

  const matchRow = {
    input_hotel_name: hotelName,
    input_hotel_name_normalized: normalizeName(hotelName),
    candidate_count: hotels.length,
    matched: matched ? 1 : 0,
    match_rule: matchRule || "",
    rakuten_hotelNo: matched?.hotelNo ?? "",
    rakuten_hotelName: matched?.hotelName ?? "",
    rakuten_areaName: matched?.areaName ?? "",
    rakuten_address1: matched?.address1 ?? "",
    rakuten_address2: matched?.address2 ?? "",
    rakuten_nearestStation: matched?.nearestStation ?? "",
    rakuten_reviewAverage: matched?.reviewAverage ?? "",
    rakuten_reviewCount: matched?.reviewCount ?? "",
  };

  const candidateRows = hotels.slice(0, auditTopN).map((candidate, index) => ({
    input_hotel_name: hotelName,
    input_hotel_name_normalized: normalizeName(hotelName),
    candidate_rank: index + 1,
    candidate_hotelNo: candidate.hotelNo ?? "",
    candidate_hotelName: candidate.hotelName ?? "",
    candidate_hotelName_normalized: normalizeName(String(candidate.hotelName || "")),
    candidate_areaName: candidate.areaName ?? "",
    candidate_address1: candidate.address1 ?? "",
    candidate_address2: candidate.address2 ?? "",
    candidate_nearestStation: candidate.nearestStation ?? "",
    candidate_reviewAverage: candidate.reviewAverage ?? "",
    candidate_reviewCount: candidate.reviewCount ?? "",
    candidate_raw_json: JSON.stringify(summarizeCandidate(candidate)),
  }));

  return { matchRow, candidateRows };
}

async function batchSimpleDetails(client, hotelNos) {
  const rows = [];
  const batches = chunkArray(hotelNos, 15);

  for (let i = 0; i < batches.length; i++) {
    log(`施設属性取得 ${i + 1}/${batches.length}`);
    const response = await client.getJson("simple_hotel_search", ENDPOINTS.simple, {
      hotelNo: batches[i].join(","),
      responseType: "large",
      hits: 30,
      page: 1,
    });

    const hotels = extractHotels(response);
    for (const hotel of hotels) {
      rows.push(
        flattenForCsv({
          ...hotel,
          rakuten_hotelNo: hotel.hotelNo ?? "",
        })
      );
    }
  }

  return rows;
}

async function batchVacant(client, hotelNos, options) {
  const rows = [];
  const batches = chunkArray(hotelNos, 15);

  for (let i = 0; i < batches.length; i++) {
    log(`空室取得 ${i + 1}/${batches.length}`);
    const response = await client.getJson("vacant_hotel_search", ENDPOINTS.vacant, {
      hotelNo: batches[i].join(","),
      checkinDate: options.checkinDate,
      checkoutDate: options.checkoutDate,
      adultNum: options.adultNum,
      roomNum: options.roomNum,
      responseType: "large",
      searchPattern: 0,
      hits: 30,
      page: 1,
    });

    const hotels = extractHotels(response);
    for (const hotel of hotels) {
      rows.push(
        flattenForCsv({
          ...hotel,
          rakuten_hotelNo: hotel.hotelNo ?? "",
          vacant_checkinDate: options.checkinDate,
          vacant_checkoutDate: options.checkoutDate,
          vacant_adultNum: options.adultNum,
          vacant_roomNum: options.roomNum,
        })
      );
    }
  }

  return rows;
}

async function getAreaClasses(client) {
  const areaJson = await client.getJson("get_area_class", ENDPOINTS.area, {});
  return {
    areaJson,
    flatRows: parseAreaRows(areaJson),
  };
}

async function runPipeline() {
  clearDownloads();
  setSummary({});
  ui.log.textContent = "";

  const appId = ui.appId.value.trim();
  const accessKey = ui.accessKey.value.trim();
  const affiliateId = ui.affiliateId.value.trim();
  const file = ui.fileInput.files[0];
  const idCol = ui.idCol.value.trim();
  const nameCol = ui.nameCol.value.trim();
  const maxRows = ui.maxRows.value.trim();
  const nameConcurrency = Math.max(1, Number(ui.nameConcurrency.value || 3));
  const rps = Math.max(0.2, Number(ui.rps.value || 1.5));
  const auditTopN = Math.max(1, Number(ui.auditTopN.value || 10));
  const maxRetries = Math.max(0, Number(ui.maxRetries.value || 4));
  const fetchVacant = ui.fetchVacant.checked;
  const checkinDate = ui.checkinDate.value;
  const checkoutDate = ui.checkoutDate.value;
  const adultNum = Math.max(1, Number(ui.adultNum.value || 2));
  const roomNum = Math.max(1, Number(ui.roomNum.value || 1));

  if (!appId || !accessKey) {
    throw new Error("applicationId と accessKey を入力してください。");
  }

  if (!file) {
    throw new Error("入力ファイルを選んでください。");
  }

  const client = new RakutenBrowserClient({
    appId,
    accessKey,
    affiliateId,
    rps,
    maxRetries,
  });

  setBadges([
    `Origin: ${location.origin}`,
    `File: ${file.name}`,
    `Concurrency: ${nameConcurrency}`,
    `RPS: ${rps}`,
    fetchVacant ? "Vacant: ON" : "Vacant: OFF",
  ]);

  log("入力ファイルを読み込みます。");
  let rows = await parseInputFile(file);

  if (maxRows) {
    rows = rows.slice(0, Number(maxRows));
  }

  if (!rows.length) {
    throw new Error("入力ファイルにデータ行がありません。");
  }

  if (!(idCol in rows[0])) {
    throw new Error(`ID列が見つかりません: ${idCol}`);
  }

  if (!(nameCol in rows[0])) {
    throw new Error(`施設名列が見つかりません: ${nameCol}`);
  }

  const inputRows = rows.map((row) => ({
    ...row,
    facilityID: row[idCol] ?? "",
    facility_name: row[nameCol] ?? "",
  }));

  const uniqueNames = [
    ...new Set(
      inputRows
        .map((row) => String(row.facility_name || "").trim())
        .filter(Boolean)
    ),
  ];

  log(`入力行数: ${inputRows.length}`);
  log(`ユニーク施設名数: ${uniqueNames.length}`);

  const limit = pLimit(nameConcurrency);
  const matchRows = [];
  const candidateRows = [];
  let processed = 0;

  log("KeywordHotelSearch を開始します。");

  await Promise.all(
    uniqueNames.map((hotelName) =>
      limit(async () => {
        const { matchRow, candidateRows: candidates } = await processOneName(
          client,
          hotelName,
          auditTopN
        );
        matchRows.push(matchRow);
        candidateRows.push(...candidates);
        processed++;

        if (processed % 10 === 0 || processed === uniqueNames.length) {
          log(`名前検索進捗 ${processed}/${uniqueNames.length}`);
        }
      })
    )
  );

  const unmatchedRows = matchRows.filter((row) => Number(row.matched) === 0);
  const matchMap = new Map(matchRows.map((row) => [String(row.input_hotel_name), row]));

  const inputWithMatch = inputRows.map((row) => ({
    ...row,
    ...(matchMap.get(String(row.facility_name)) || {}),
  }));

  const matchedHotelNos = [
    ...new Set(
      inputWithMatch
        .map((row) => String(row.rakuten_hotelNo || "").trim())
        .filter(Boolean)
    ),
  ];

  log(`マッチしたユニーク施設名: ${matchRows.filter((row) => Number(row.matched) === 1).length}`);
  log(`未一致ユニーク施設名: ${unmatchedRows.length}`);
  log(`ユニーク rakuten_hotelNo 数: ${matchedHotelNos.length}`);

  log("GetAreaClass を取得します。");
  const { areaJson, flatRows: areaFlatRows } = await getAreaClasses(client);

  let hotelDetails = [];
  let inputWithDetails = inputWithMatch;

  if (matchedHotelNos.length > 0) {
    hotelDetails = await batchSimpleDetails(client, matchedHotelNos);
    const detailMap = new Map(
      hotelDetails.map((row) => [String(row.rakuten_hotelNo || ""), row])
    );
    inputWithDetails = joinByLookup(
      inputWithMatch,
      "rakuten_hotelNo",
      detailMap,
      "detail__"
    );
  }

  let vacantRows = [];
  if (fetchVacant && matchedHotelNos.length > 0) {
    log("VacantHotelSearch を取得します。");
    vacantRows = await batchVacant(client, matchedHotelNos, {
      checkinDate,
      checkoutDate,
      adultNum,
      roomNum,
    });
  }

  const summary = {
    input_rows: inputRows.length,
    unique_input_hotel_names: uniqueNames.length,
    matched_unique_names: matchRows.filter((row) => Number(row.matched) === 1).length,
    unmatched_unique_names: unmatchedRows.length,
    matched_unique_rakuten_hotelNos: matchedHotelNos.length,
    hotel_details_rows: hotelDetails.length,
    vacant_rows: vacantRows.length,
    output_note: "vacant_rows は部屋・プラン単位で複数行になることがあります。",
  };

  setSummary(summary);

  createCsvDownload("01_name_match_results.csv", matchRows);
  createCsvDownload("02_candidate_audit.csv", candidateRows);
  createCsvDownload("03_unmatched_names.csv", unmatchedRows);
  createCsvDownload("04_input_with_rakuten_match.csv", inputWithMatch);
  createJsonDownload("05_area_classes_raw.json", areaJson);
  createCsvDownload("06_area_classes_flat.csv", areaFlatRows);
  createCsvDownload("07_hotel_details.csv", hotelDetails);
  createCsvDownload("08_input_with_details.csv", inputWithDetails);

  if (fetchVacant) {
    createCsvDownload("09_vacant_hotels.csv", vacantRows);
  }

  createJsonDownload("10_run_summary.json", summary);

  log("完了しました。ダウンロードリンクから保存してください。", "ok");
}
