using AgCode.Business.Providers.Interfaces;
using AgCode.Core.Data.Dto;
using AgCode.Core.Data.Repository.ClientDatabaseModel;
using AgCode.Core.Interfaces;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AgCode.Business.Providers
{
    public class ActivityCodeProvider : IActivityCodeProvider
    {
        private IRepository<ActivityCodeCategory> _activityCodeCategoryRepo;
        private IRepository<ActivityCode> _activityCodeRepo;
        private IRepository<PayType> _payTypeRepo;
        private readonly IRepository<PersonnelType> _personnelTypeRepo;
        private IRepository<TaskEquipmentType> _taskEquipmentTypeRepo;
        private IRepository<TaskPersonnelType> _taskPersonnelTypeRepo;
        private IRepository<WorkTaskProduct> _workTaskProductRepo;
        private IRepository<TaskDetail> _taskDetailRepo;
        private IRepository<Season> _seasonRepo;
        private IRepository<Block> _blockRepo;
        private IRepository<Ranch> _ranchRepo;
        private IRepository<Unit> _unitRepo;
        private IRepository<ClientDivision> _clientDivisionRepo;
        private IRepository<SubBlock> _subBlockRepo;
        private IRepository<CropType> _cropTypeRepo;

        public ActivityCodeProvider(IRepository<ActivityCodeCategory> activityCodeCategoryRepo,
            IRepository<ActivityCode> activityCodeRepo,
            IRepository<PayType> payTypeRepo,
            IRepository<PersonnelType> personnelTypeRepo,
            IRepository<TaskEquipmentType> taskEquipmentTypeRepo,
            IRepository<TaskPersonnelType> taskPersonnelTypeRepo,
            IRepository<WorkTaskProduct> workTaskProductRepo,
            IRepository<TaskDetail> taskDetailRepo,
            IRepository<Season> seasonRepo,
            IRepository<Block> blockRepo,
            IRepository<Ranch> ranchRepo,
            IRepository<Unit> unitRepo,
            IRepository<ClientDivision> clientDivisionRepo,
            IRepository<SubBlock> subBlockRepo,
            IRepository<CropType> CropTypeRepo)
        {

            _activityCodeCategoryRepo = activityCodeCategoryRepo;
            _activityCodeRepo = activityCodeRepo;
            _payTypeRepo = payTypeRepo;
            _personnelTypeRepo = personnelTypeRepo;
            _taskEquipmentTypeRepo = taskEquipmentTypeRepo;
            _taskPersonnelTypeRepo = taskPersonnelTypeRepo;
            _workTaskProductRepo = workTaskProductRepo;
            _taskDetailRepo = taskDetailRepo;
            _seasonRepo = seasonRepo;
            _blockRepo = blockRepo;
            _ranchRepo = ranchRepo;
            _unitRepo = unitRepo;
            _clientDivisionRepo = clientDivisionRepo;
            _subBlockRepo = subBlockRepo;
            _cropTypeRepo = CropTypeRepo;
        }
        #region ActivityCodeCategories
        public IEnumerable<ActivityCodeCategoryDto> GetActivityCodeCategories()
        {
            return _activityCodeCategoryRepo.Get(x => x.Deleted == null).Select(x => new ActivityCodeCategoryDto
            {
                Id = x.ActivityCodeCategoryID,
                ShortName = x.ShortName,
                LongName = x.LongName
            }).OrderBy(x => x.ShortName);
        }
        public ActivityCodeCategoryDto GetActivityCodeCategoryById(int id)
        {
            return _activityCodeCategoryRepo.Get(x => x.Deleted == null && x.ActivityCodeCategoryID == id).Select(x => new ActivityCodeCategoryDto
            {
                Id = x.ActivityCodeCategoryID,
                ShortName = x.ShortName,
                LongName = x.LongName
            }).FirstOrDefault();

        }
        public async Task<int> UpdateActivityCategoryCodeAsync(ActivityCodeCategoryDto categoryDto)
        {
            var existingActivityCode = _activityCodeCategoryRepo.Get(x => x.ActivityCodeCategoryID == categoryDto.Id);
            if (existingActivityCode != null)
            {
                ActivityCodeCategory categoryCode = new ActivityCodeCategory();
                categoryCode.ActivityCodeCategoryID = categoryDto.Id;
                categoryCode.ShortName = categoryDto.ShortName ?? categoryDto.LongName;
                categoryCode.LongName = categoryDto.LongName ?? categoryDto.ShortName;
                _activityCodeCategoryRepo.Update(categoryCode);
                await _activityCodeCategoryRepo.SaveChanges().ConfigureAwait(true);
                return categoryCode.ActivityCodeCategoryID;
            }
            return -1;
        }
        public async Task<int> InsertActivityCategoryCodeAsync(ActivityCodeCategoryDto categoryDto)
        {
            if (categoryDto != null)
            {
                ActivityCodeCategory categoryCode = new ActivityCodeCategory();
                categoryCode.ShortName = categoryDto.ShortName ?? categoryDto.LongName;
                categoryCode.LongName = categoryDto.LongName ?? categoryDto.ShortName;
                _activityCodeCategoryRepo.Insert(categoryCode);
                await _activityCodeCategoryRepo.SaveChanges().ConfigureAwait(true);
                return categoryCode.ActivityCodeCategoryID;
            }
            return -1;
        }
        #endregion

        public ActivityCodeDto GetActivityCodeById(int id)
        {
            IQueryable<ActivityCode> activityCodes = _activityCodeRepo.GetQuery();
            IQueryable<ActivityCodeCategory> activityCodeCategories = _activityCodeCategoryRepo.GetQuery();
            var result = from activityCode in activityCodes
                         join codeCategory in activityCodeCategories on activityCode.ActivityCodeCategoryID equals codeCategory.ActivityCodeCategoryID
                         where activityCode.Deleted == null && !activityCode.Inactive && activityCode.ActivityCodeID == id
                         orderby activityCode.ShortName
                         select new ActivityCodeDto
                         {
                             Id = activityCode.ActivityCodeID,
                             //Main Properties(Missing Activity Type)
                             ActivityCodeCategoryId = activityCode.ActivityCodeCategoryID,
                             ActivityCodeCategoryName = codeCategory.ShortName,
                             // TODO Logic to get code number
                             ActivityCodeNumber = activityCode.ActivityCodeNumber,
                             ShortName = activityCode.ShortName,
                             LongName = activityCode.LongName,
                             Sequence = activityCode.Sequence,
                             InActive = activityCode.Inactive,
                             IsPesticide = activityCode.IsPesticide,//Reccommendation
                             IsHarvest = activityCode.IsHarvest,
                             IsScouting = activityCode.IsFieldInspection,
                             IsTaskLoad = activityCode.IsTaskLoad,
                             //Payroll
                             PayTypeId = activityCode.PayTypeID,
                             HourlyRate = activityCode.HourlyRate,
                             PieceRate = activityCode.PieceRate,
                             PieceUOMId = activityCode.PieceUOMID,
                             IsShiftDifferential = activityCode.IsShiftDifferential,
                             ShiftDifferentialRate = activityCode.ShiftDifferentialRate,
                             IsAdjustmentCode = activityCode.IsAdjustmentCode,
                             IsAttendanceCode = activityCode.IsAttendanceCode,
                             PremiumPercent = activityCode.PremiumPercent,
                             PremiumRate = activityCode.PremiumRate,
                             //Workflow
                             //Viewable Tabs(missin 3)
                             IsPersonnelTab = activityCode.IsPersonnelTab,
                             IsPersonnelDetailTab = activityCode.IsPersonnelDetailTab,
                             IsMaterialTab = activityCode.IsMaterialTab,
                             IsEquipmentTab = activityCode.IsEquipmentTab,
                             IsDetailTab = activityCode.IsDetailTab,
                             //Other(missing 4)
                             IsEquipmentInspection = activityCode.IsEquipmentInspection,
                             IsMaintenance = activityCode.IsMaintenance,
                             IsMaintenanceRepair = activityCode.IsMaintenanceRepair,
                             IsBillableDefault = activityCode.IsBillableDefault,
                             PassTypeID = activityCode.PassTypeID,
                             InternalReferenceID = activityCode.InternalReferenceID,
                             //Rules
                             IsAlwaysOpen = activityCode.IsAlwaysOpen,
                             IsAutoComplete = activityCode.IsAutoComplete,
                             IsCalcWork = activityCode.IsCalcWork,
                             IsCrewWork = activityCode.IsCrewWork,
                             IsAutoRecordWork = activityCode.IsAutoRecordWork,
                             //Logical Entities
                             IsStandard = activityCode.IsWorkTab && !activityCode.IsWorkDistribution,
                             IsDistribution = activityCode.IsWorkTab && activityCode.IsWorkDistribution,
                             IsNoWork = !activityCode.IsWorkTab && !activityCode.IsWorkDistribution && !activityCode.IsAutoRecordWork,
                             //PayCode
                             CrewDefault = activityCode.IsWorkTab && activityCode.IsWorkDistribution ? "Dsitribution" : activityCode.IsWorkTab ? "Standard" : activityCode.IsAutoRecordWork ? "Auto Record" : "No Work",
                             WorkEntry = activityCode.IsCrewWork ? "Crew" : activityCode.IsWorkTab && activityCode.IsWorkDistribution ? "Personnel" : string.Empty
                         };
            return result.FirstOrDefault();

        }
        public IEnumerable<ActivityCodeDto> GetActivityCodes()
        {
            IQueryable<ActivityCode> activityCodes = _activityCodeRepo.GetQuery();
            IQueryable<PayType> payTypes = _payTypeRepo.GetQuery();
            IQueryable<ActivityCodeCategory> activityCodeCategories = _activityCodeCategoryRepo.GetQuery();
            return from activityCode in activityCodes
                   join codeCategory in activityCodeCategories on activityCode.ActivityCodeCategoryID equals codeCategory.ActivityCodeCategoryID
                   join payType in payTypes on activityCode.PayTypeID equals payType.PayTypeID into z
                   from payTypeOutput in z.DefaultIfEmpty()
                   where activityCode.Deleted == null && !activityCode.Inactive
                   orderby activityCode.ShortName
                   select new ActivityCodeDto
                   {
                       Id = activityCode.ActivityCodeID,
                       //Main Properties(Missing Activity Type)
                       ActivityCodeCategoryId = activityCode.ActivityCodeCategoryID,
                       ActivityCodeCategoryName = codeCategory.ShortName,
                       // TODO Logic to get code number
                       ActivityCodeNumber = activityCode.ActivityCodeNumber,
                       ShortName = activityCode.ShortName,
                       LongName = activityCode.LongName,
                       Sequence = activityCode.Sequence,
                       InActive = activityCode.Inactive,
                       IsPesticide = activityCode.IsPesticide,//Reccommendation
                       IsHarvest = activityCode.IsHarvest,
                       IsScouting = activityCode.IsFieldInspection,
                       IsTaskLoad = activityCode.IsTaskLoad,
                       //Payroll
                       PayTypeId = activityCode.PayTypeID,
                       PayTypeName = payTypeOutput.Description,
                       HourlyRate = activityCode.HourlyRate,
                       PieceRate = activityCode.PieceRate,
                       PieceUOMId = activityCode.PieceUOMID,
                       IsShiftDifferential = activityCode.IsShiftDifferential,
                       ShiftDifferentialRate = activityCode.ShiftDifferentialRate,
                       IsAdjustmentCode = activityCode.IsAdjustmentCode,
                       IsAttendanceCode = activityCode.IsAttendanceCode,
                       PremiumPercent = activityCode.PremiumPercent,
                       PremiumRate = activityCode.PremiumRate,
                       //Workflow
                       //Viewable Tabs(missin 3)
                       IsPersonnelTab = activityCode.IsPersonnelTab,
                       IsPersonnelDetailTab = activityCode.IsPersonnelDetailTab,
                       IsMaterialTab = activityCode.IsMaterialTab,
                       IsEquipmentTab = activityCode.IsEquipmentTab,
                       IsDetailTab = activityCode.IsDetailTab,
                       //Other(missing 4)
                       IsEquipmentInspection = activityCode.IsEquipmentInspection,
                       IsMaintenance = activityCode.IsMaintenance,
                       IsMaintenanceRepair = activityCode.IsMaintenanceRepair,
                       IsBillableDefault = activityCode.IsBillableDefault,
                       PassTypeID = activityCode.PassTypeID,
                       InternalReferenceID = activityCode.InternalReferenceID,
                       //Rules
                       IsAlwaysOpen = activityCode.IsAlwaysOpen,
                       IsAutoComplete = activityCode.IsAutoComplete,
                       IsCalcWork = activityCode.IsCalcWork,
                       IsCrewWork = activityCode.IsCrewWork,
                       IsAutoRecordWork = activityCode.IsAutoRecordWork,
                       //PayCode
                       CrewDefault = activityCode.IsWorkTab && activityCode.IsWorkDistribution ? "Dsitribution" : activityCode.IsWorkTab ? "Standard" : activityCode.IsAutoRecordWork ? "Auto Record" : "No Work",
                       WorkEntry = activityCode.IsCrewWork ? "Crew" : activityCode.IsWorkTab && activityCode.IsWorkDistribution ? "Personnel" : string.Empty,

                       //Logical Entities
                       IsStandard = activityCode.IsWorkTab && !activityCode.IsWorkDistribution,
                       IsDistribution = activityCode.IsWorkTab && activityCode.IsWorkDistribution,
                       IsNoWork = !activityCode.IsWorkTab && !activityCode.IsWorkDistribution && !activityCode.IsAutoRecordWork,
                   };
        }

        public async Task<int> UpdateActivityCodeAsync(ActivityCodeDto activityCodeDto)
        {
            var existingActivityCode = _activityCodeRepo.Get(x => x.ActivityCodeID == activityCodeDto.Id);
            if (existingActivityCode != null)
            {
                ActivityCode activityCode = new ActivityCode();
                activityCode.ActivityCodeID = activityCodeDto.Id;


                //Main Properties(Missing Activity Type)
                activityCode.ActivityCodeCategoryID = activityCodeDto.ActivityCodeCategoryId;
                activityCode.ActivityCodeNumber = activityCodeDto.ActivityCodeNumber;
                activityCode.ShortName = string.IsNullOrEmpty(activityCodeDto.ShortName) ? activityCodeDto.LongName : activityCodeDto.ShortName;
                activityCode.LongName = string.IsNullOrEmpty(activityCodeDto.LongName) ? activityCodeDto.ShortName : activityCodeDto.LongName;
                activityCode.Sequence = activityCodeDto.Sequence;
                activityCode.Inactive = activityCodeDto.InActive;
                activityCode.IsPesticide = activityCodeDto.IsPesticide;//Reccommendation
                activityCode.IsHarvest = activityCodeDto.IsHarvest;
                activityCode.IsFieldInspection = activityCodeDto.IsScouting;
                activityCode.IsTaskLoad = activityCodeDto.IsTaskLoad;
                //Payroll
                activityCode.PayTypeID = activityCodeDto.PayTypeId == 0 ? null : activityCodeDto.PayTypeId;
                activityCode.HourlyRate = activityCodeDto.HourlyRate;
                activityCode.PieceRate = activityCodeDto.PieceRate;
                activityCode.PieceUOMID = activityCodeDto.PieceUOMId == 0 ? null : activityCodeDto.PieceUOMId;
                activityCode.IsShiftDifferential = activityCodeDto.IsShiftDifferential;
                activityCode.ShiftDifferentialRate = activityCodeDto.ShiftDifferentialRate;
                activityCode.IsAdjustmentCode = activityCodeDto.IsAdjustmentCode;
                activityCode.IsAttendanceCode = activityCodeDto.IsAttendanceCode;
                activityCode.PremiumPercent = activityCodeDto.PremiumPercent;
                activityCode.PremiumRate = activityCodeDto.PremiumRate;
                //Workflow
                //Viewable Tabs(missin 3)
                activityCode.IsPersonnelTab = activityCodeDto.IsPersonnelTab;
                activityCode.IsPersonnelDetailTab = activityCodeDto.IsPersonnelDetailTab;
                activityCode.IsMaterialTab = activityCodeDto.IsMaterialTab;
                activityCode.IsEquipmentTab = activityCodeDto.IsEquipmentTab;
                activityCode.IsDetailTab = activityCodeDto.IsDetailTab;
                //Other(missing 4)
                activityCode.IsEquipmentInspection = activityCodeDto.IsEquipmentInspection;
                activityCode.IsMaintenance = activityCodeDto.IsMaintenance;
                activityCode.IsMaintenanceRepair = activityCodeDto.IsMaintenanceRepair;
                activityCode.IsBillableDefault = activityCodeDto.IsBillableDefault;
                activityCode.PassTypeID = activityCodeDto.PassTypeID;
                activityCode.InternalReferenceID = activityCodeDto.InternalReferenceID == 0 ? null : activityCode.InternalReferenceID;
                //Rules
                activityCode.IsAlwaysOpen = activityCodeDto.IsAlwaysOpen;
                activityCode.IsAutoComplete = activityCodeDto.IsAutoComplete;
                //Mapping Logical Entities
                if (activityCodeDto.IsStandard)
                {
                    activityCode.IsWorkTab = true;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = activityCodeDto.IsCalcWork;
                    activityCode.IsCrewWork = activityCodeDto.IsCrewWork;
                }
                else if (activityCodeDto.IsAutoRecordWork)
                {
                    activityCode.IsWorkTab = false;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = true;
                    activityCode.IsCalcWork = false;
                    activityCode.IsCrewWork = false;
                }
                else if (activityCodeDto.IsDistribution)
                {
                    activityCode.IsWorkTab = true;
                    activityCode.IsWorkDistribution = true;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = activityCodeDto.IsCalcWork;
                    activityCode.IsCrewWork = false;
                }
                else if (activityCodeDto.IsNoWork)
                {
                    activityCode.IsWorkTab = false;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = false;
                    activityCode.IsCrewWork = false;
                }

                _activityCodeRepo.Update(activityCode);
                await _activityCodeRepo.SaveChanges().ConfigureAwait(true);
                return activityCode.ActivityCodeID;

            }
            else
            {
                return -1;
            }
        }
        public async Task<int> InsertActivityCodeAsync(ActivityCodeDto activityCodeDto)
        {

            if (activityCodeDto != null)
            {
                ActivityCode activityCode = new ActivityCode();
                activityCode.ActivityCodeID = activityCodeDto.Id;


                //Main Properties(Missing Activity Type)
                activityCode.ActivityCodeCategoryID = activityCodeDto.ActivityCodeCategoryId;
                activityCode.ActivityCodeNumber = activityCodeDto.ActivityCodeNumber;
                activityCode.ShortName = string.IsNullOrEmpty(activityCodeDto.ShortName) ? activityCodeDto.LongName : activityCodeDto.ShortName;
                activityCode.LongName = string.IsNullOrEmpty(activityCodeDto.LongName) ? activityCodeDto.ShortName : activityCodeDto.LongName;
                activityCode.Sequence = activityCodeDto.Sequence;
                activityCode.Inactive = activityCodeDto.InActive;
                activityCode.IsPesticide = activityCodeDto.IsPesticide;//Reccommendation
                activityCode.IsHarvest = activityCodeDto.IsHarvest;
                activityCode.IsFieldInspection = activityCodeDto.IsScouting;
                activityCode.IsTaskLoad = activityCodeDto.IsTaskLoad;
                //Payroll
                activityCode.PayTypeID = activityCodeDto.PayTypeId == 0 ? null : activityCodeDto.PayTypeId;
                activityCode.HourlyRate = activityCodeDto.HourlyRate;
                activityCode.PieceRate = activityCodeDto.PieceRate;
                activityCode.PieceUOMID = activityCodeDto.PieceUOMId == 0 ? null : activityCodeDto.PieceUOMId;
                activityCode.IsShiftDifferential = activityCodeDto.IsShiftDifferential;
                activityCode.ShiftDifferentialRate = activityCodeDto.ShiftDifferentialRate;
                activityCode.IsAdjustmentCode = activityCodeDto.IsAdjustmentCode;
                activityCode.IsAttendanceCode = activityCodeDto.IsAttendanceCode;
                activityCode.PremiumPercent = activityCodeDto.PremiumPercent;
                activityCode.PremiumRate = activityCodeDto.PremiumRate;
                //Workflow
                //Viewable Tabs(missin 3)
                activityCode.IsPersonnelTab = activityCodeDto.IsPersonnelTab;
                activityCode.IsPersonnelDetailTab = activityCodeDto.IsPersonnelDetailTab;
                activityCode.IsMaterialTab = activityCodeDto.IsMaterialTab;
                activityCode.IsEquipmentTab = activityCodeDto.IsEquipmentTab;
                activityCode.IsDetailTab = activityCodeDto.IsDetailTab;
                //Other(missing 4)
                activityCode.IsEquipmentInspection = activityCodeDto.IsEquipmentInspection;
                activityCode.IsMaintenance = activityCodeDto.IsMaintenance;
                activityCode.IsMaintenanceRepair = activityCodeDto.IsMaintenanceRepair;
                activityCode.IsBillableDefault = activityCodeDto.IsBillableDefault;
                activityCode.PassTypeID = activityCodeDto.PassTypeID;
                activityCode.InternalReferenceID = activityCodeDto.InternalReferenceID == 0 ? null : activityCode.InternalReferenceID;
                //Rules
                activityCode.IsAlwaysOpen = activityCodeDto.IsAlwaysOpen;
                activityCode.IsAutoComplete = activityCodeDto.IsAutoComplete;
                //Mapping Logical Entities
                if (activityCodeDto.IsStandard)
                {
                    activityCode.IsWorkTab = true;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = activityCodeDto.IsCalcWork;
                    activityCode.IsCrewWork = activityCodeDto.IsCrewWork;
                }
                else if (activityCodeDto.IsAutoRecordWork)
                {
                    activityCode.IsWorkTab = false;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = true;
                    activityCode.IsCalcWork = false;
                    activityCode.IsCrewWork = false;
                }
                else if (activityCodeDto.IsDistribution)
                {
                    activityCode.IsWorkTab = true;
                    activityCode.IsWorkDistribution = true;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = activityCodeDto.IsCalcWork;
                    activityCode.IsCrewWork = false;
                }
                else if (activityCodeDto.IsNoWork)
                {
                    activityCode.IsWorkTab = false;
                    activityCode.IsWorkDistribution = false;
                    activityCode.IsAutoRecordWork = false;
                    activityCode.IsCalcWork = false;
                    activityCode.IsCrewWork = false;
                }
                _activityCodeRepo.Insert(activityCode);
                await _activityCodeRepo.SaveChanges().ConfigureAwait(true);
                return activityCode.ActivityCodeID;
            }
            else
            {
                return -1;
            }
        }

        public IEnumerable<OptionsDto> GetActivityCodeOptions()
        {
            IEnumerable<OptionsDto> result = null;

            var activityCodes = GetActivityCodes();
            if (activityCodes != null)
            {
                result = activityCodes.Select(x => new OptionsDto
                {
                    Id = x.Id,
                    Name = x.ShortName
                });

            }

            return result;
        }

        public IEnumerable<ActivityCodeByMonthDto> GetActivityCodeByMonths()
        {
            IQueryable<PersonnelType> personnelTypes = _personnelTypeRepo.GetQuery();
            IQueryable<TaskEquipmentType> taskEquipmentTypes = _taskEquipmentTypeRepo.GetQuery();
            IQueryable<TaskPersonnelType> taskPersonnelTypes = _taskPersonnelTypeRepo.GetQuery();
            IQueryable<WorkTaskProduct> workTaskProducts = _workTaskProductRepo.GetQuery();
            IQueryable<TaskDetail> taskDetails = _taskDetailRepo.GetQuery();
            IQueryable<ActivityCode> activityCodes = _activityCodeRepo.GetQuery();
            IQueryable<ActivityCodeCategory> activityCodeCategories = _activityCodeCategoryRepo.GetQuery();
            IQueryable<Season> seasons = _seasonRepo.GetQuery(s => s.Deleted == null);
            IQueryable<Block> blocks = _blockRepo.GetQuery();
            IQueryable<Ranch> ranches = _ranchRepo.GetQuery();
            IQueryable<Unit> units = _unitRepo.GetQuery();
            IQueryable<ClientDivision> clientDivisions = _clientDivisionRepo.GetQuery();
            IQueryable<SubBlock> subBlocks = _subBlockRepo.GetQuery();
            IQueryable<CropType> cropTypes = _cropTypeRepo.GetQuery();

            var taskInternalLaborQuery = from tpt in taskPersonnelTypes
                                         join pt in personnelTypes
                                         on tpt.PersonnelTypeID equals pt.PersonnelTypeId
                                         where pt.LaborForceTypeId == 2
                                         select new { TaskId = tpt.TaskID, Type = "Internal Labor", Total = tpt.CostPerGeoUnit };

            var taskExternalLaborQuery = from tpt in taskPersonnelTypes
                                         join pt in personnelTypes
                                         on tpt.PersonnelTypeID equals pt.PersonnelTypeId
                                         where pt.LaborForceTypeId != 2
                                         select new { TaskId = tpt.TaskID, Type = "External Labor", Total = tpt.CostPerGeoUnit };

            var taskEquipmentQuery = from tet in taskEquipmentTypes
                                     where tet.Deleted == null
                                     select new { TaskId = tet.TaskID, Type = "Equipment", Total = tet.CostPerGeoUnit ?? decimal.Zero };

            var taskMaterialQuery = from wtp in workTaskProducts
                                    where wtp.Deleted == null
                                    select new { TaskId = wtp.TaskID, Type = "Material", Total = wtp.CostPerGeoUnit };

            var taskTypeQuery = taskInternalLaborQuery.Union(taskExternalLaborQuery).Union(taskEquipmentQuery).Union(taskMaterialQuery);

            return from taskDetail in taskDetails
                   join taskType in taskTypeQuery on taskDetail.TaskID equals taskType.TaskId
                   join activityCode in activityCodes on taskDetail.ActivityCodeID equals activityCode.ActivityCodeID
                   join activityCodeCategorie in activityCodeCategories on activityCode.ActivityCodeCategoryID equals activityCodeCategorie.ActivityCodeCategoryID
                       into activity
                   from acc in activity.DefaultIfEmpty()
                   join season in seasons on taskDetail.SeasonID equals season.SeasonID
                   join block in blocks on season.L1PDUnitID equals block.L1PDUnitID
                   join ranche in ranches on block.L0PDUnitID equals ranche.L0PDUnitID
                   join unit in units on ranche.PDUnitID equals unit.PDUnitId
                   join clientDivision in clientDivisions on unit.DivisionId equals clientDivision.DivisionId
                   join subBlock in subBlocks on season.L2PDUnitID equals subBlock.L2PDUnitID
                       into sB
                   from sb in sB.DefaultIfEmpty()
                   join cropType in cropTypes on season.CropTypeID equals cropType.CropTypeID
                       into cT
                   from ct in cT.DefaultIfEmpty()
                   where taskDetail.Deleted == null && taskDetail.IsFieldOrder == false
                   group new { sb, clientDivision, unit, ranche, block, season, ct, activity, activityCode, taskType, taskDetail }
                   by new
                   {
                       sbShortname = sb.ShortName,
                       clientDivisionShortName = clientDivision.ShortName,
                       unitShortName = unit.ShortName,
                       rancheShortName = ranche.ShortName,
                       blockShortName = block.ShortName,
                       seasonSeasonYear = season.SeasonYear,
                       ctShortName = ct.ShortName,
                       seasonAcres = season.Acres,
                       seasonProductionUnits = season.ProductionUnits,
                       activityCodeShortName = activityCode.ShortName,
                       accShortName = acc.ShortName,
                       taskTypeType = taskType.Type
                   }
                        into tableGroup
                   select new ActivityCodeByMonthDto
                   {
                       Id = 0,
                       Season = tableGroup.Key.seasonSeasonYear,
                       Unit = tableGroup.Key.unitShortName,
                       Ranch = tableGroup.Key.rancheShortName,
                       Block = tableGroup.Key.blockShortName,
                       Variety = tableGroup.Key.ctShortName,
                       Acres = tableGroup.Key.seasonAcres ?? decimal.Zero,
                       Vines = tableGroup.Key.seasonProductionUnits ?? decimal.Zero,
                       Category = tableGroup.Key.accShortName,
                       Activity = tableGroup.Key.activityCodeShortName,
                       Type = tableGroup.Key.taskTypeType,
                       Total = tableGroup.Sum(d => d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * (d.taskDetail.PercentToComplete ?? 100) / 100) : decimal.Zero)),
                       January = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 1 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       February = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 2 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       March = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 3 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       April = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 4 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       May = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 5 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       June = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 6 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       July = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 7 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       August = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 8 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       September = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 9 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       October = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 10 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       November = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 11 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero),
                       December = tableGroup.Sum(d => d.taskDetail.PlannedFor != null && d.taskDetail.PlannedFor.Value.Month == 12 ? (d.taskType.Total * (d.taskDetail.IsOverhead == false ? (d.season.Acres ?? decimal.Zero * d.taskDetail.PercentToComplete ?? 100 / 100) : decimal.One)) : decimal.Zero)
                   };
        }
    }
}
